#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/fs.h>
#include <linux/miscdevice.h>
#include <linux/uaccess.h>
#include <linux/list.h>
#include <linux/slab.h>
#include <linux/mutex.h>
#include <linux/kthread.h>
#include <linux/delay.h>
#include <linux/mm.h>
#include <linux/sched/signal.h>

#include "monitor_ioctl.h"

MODULE_LICENSE("GPL");

struct monitored_container {
    pid_t pid;
    size_t soft_limit_bytes;
    size_t hard_limit_bytes;
    int soft_warned;
    struct list_head list;
};

static LIST_HEAD(container_list);
static DEFINE_MUTEX(container_list_lock);
static struct task_struct *monitor_thread;

/* ================= DEVICE ================= */

static int monitor_open(struct inode *inode, struct file *file) {
    return 0;
}

static int monitor_release(struct inode *inode, struct file *file) {
    return 0;
}

static long monitor_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
    struct monitor_request info;
    struct monitored_container *entry;
    pid_t pid;

    switch (cmd) {

    case MONITOR_REGISTER:
        if (copy_from_user(&info, (void __user *)arg, sizeof(info)))
            return -EFAULT;

        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid = info.pid;
        entry->soft_limit_bytes = info.soft_limit_bytes;
        entry->hard_limit_bytes = info.hard_limit_bytes;
        entry->soft_warned = 0;

        mutex_lock(&container_list_lock);
        list_add(&entry->list, &container_list);
        mutex_unlock(&container_list_lock);

        printk(KERN_INFO "[monitor] Registered PID %d\n", info.pid);
        break;

    case MONITOR_UNREGISTER:
        if (copy_from_user(&info, (void __user *)arg, sizeof(info)))
            return -EFAULT;

        pid = info.pid;

        mutex_lock(&container_list_lock);

        {
            struct monitored_container *e, *tmp;
            list_for_each_entry_safe(e, tmp, &container_list, list) {
                if (e->pid == pid) {
                    list_del(&e->list);
                    kfree(e);
                    break;
                }
            }
        }

        mutex_unlock(&container_list_lock);
        break;

    default:
        return -EINVAL;
    }

    return 0;
}

static const struct file_operations fops = {
    .owner = THIS_MODULE,
    .open = monitor_open,
    .release = monitor_release,
    .unlocked_ioctl = monitor_ioctl,
};

static struct miscdevice monitor_dev = {
    .minor = MISC_DYNAMIC_MINOR,
    .name = "container_monitor",
    .fops = &fops,
};

/* ================= THREAD ================= */

static int monitor_fn(void *data)
{
    while (!kthread_should_stop()) {

        mutex_lock(&container_list_lock);

        {
            struct monitored_container *entry, *tmp;

            list_for_each_entry_safe(entry, tmp, &container_list, list) {

                struct task_struct *task =
                    get_pid_task(find_vpid(entry->pid), PIDTYPE_PID);

                if (!task) {
                    list_del(&entry->list);
                    kfree(entry);
                    continue;
                }

                struct mm_struct *mm = get_task_mm(task);

                if (mm) {
                    unsigned long rss = get_mm_rss(mm) << PAGE_SHIFT;

                    if (rss > entry->hard_limit_bytes) {
                        printk(KERN_ERR "[monitor] Killing PID %d\n", entry->pid);
                        send_sig(SIGKILL, task, 1);
                    }
                    else if (rss > entry->soft_limit_bytes && !entry->soft_warned) {
                        printk(KERN_WARNING "[monitor] Soft limit exceeded PID %d\n", entry->pid);
                        entry->soft_warned = 1;
                    }

                    mmput(mm);
                }

                put_task_struct(task);
            }
        }

        mutex_unlock(&container_list_lock);

        msleep(1000);
    }

    return 0;
}

/* ================= INIT / EXIT ================= */

static int __init monitor_init(void) {
    printk(KERN_INFO "[monitor] Module loaded\n");

    if (misc_register(&monitor_dev) != 0) {
        printk(KERN_ERR "[monitor] Failed to register device\n");
        return -1;
    }

    monitor_thread = kthread_run(monitor_fn, NULL, "monitor_thread");
    if (IS_ERR(monitor_thread)) {
        printk(KERN_ERR "[monitor] Failed to create thread\n");
        misc_deregister(&monitor_dev);
        return PTR_ERR(monitor_thread);
    }

    return 0;
}

static void __exit monitor_exit(void) {
    struct monitored_container *entry, *tmp;

    if (monitor_thread)
        kthread_stop(monitor_thread);

    mutex_lock(&container_list_lock);

    list_for_each_entry_safe(entry, tmp, &container_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }

    mutex_unlock(&container_list_lock);

    misc_deregister(&monitor_dev);

    printk(KERN_INFO "[monitor] Module unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);


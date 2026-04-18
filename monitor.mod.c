#include <linux/module.h>
#include <linux/export-internal.h>
#include <linux/compiler.h>

MODULE_INFO(name, KBUILD_MODNAME);

__visible struct module __this_module
__section(".gnu.linkonce.this_module") = {
	.name = KBUILD_MODNAME,
	.init = init_module,
#ifdef CONFIG_MODULE_UNLOAD
	.exit = cleanup_module,
#endif
	.arch = MODULE_ARCH_INIT,
};



static const struct modversion_info ____versions[]
__used __section("__versions") = {
	{ 0xd5ad82a1, "misc_deregister" },
	{ 0xbd03ed67, "__ref_stack_chk_guard" },
	{ 0x092a35a2, "_copy_from_user" },
	{ 0xf46d5bf3, "mutex_lock" },
	{ 0xcb8b6ec6, "kfree" },
	{ 0xf46d5bf3, "mutex_unlock" },
	{ 0xbd03ed67, "random_kmalloc_seed" },
	{ 0xfaabfe5e, "kmalloc_caches" },
	{ 0xc064623f, "__kmalloc_cache_noprof" },
	{ 0xd272d446, "__stack_chk_fail" },
	{ 0x0571dc46, "kthread_stop" },
	{ 0x5e505530, "kthread_should_stop" },
	{ 0xc13edbcb, "find_vpid" },
	{ 0x848a0d8d, "get_pid_task" },
	{ 0xbf2c538b, "get_task_mm" },
	{ 0xcf46e6bd, "mmput" },
	{ 0x1cf09ab5, "__put_task_struct_rcu_cb" },
	{ 0xb9fcd065, "call_rcu" },
	{ 0x8ffd462b, "send_sig" },
	{ 0x67628f51, "msleep" },
	{ 0x2520ea93, "refcount_warn_saturate" },
	{ 0xd272d446, "__fentry__" },
	{ 0xd272d446, "__x86_return_thunk" },
	{ 0xe8213e80, "_printk" },
	{ 0xaca12394, "misc_register" },
	{ 0x7f79e79a, "kthread_create_on_node" },
	{ 0x630dad60, "wake_up_process" },
	{ 0xbebe66ff, "module_layout" },
};

static const u32 ____version_ext_crcs[]
__used __section("__version_ext_crcs") = {
	0xd5ad82a1,
	0xbd03ed67,
	0x092a35a2,
	0xf46d5bf3,
	0xcb8b6ec6,
	0xf46d5bf3,
	0xbd03ed67,
	0xfaabfe5e,
	0xc064623f,
	0xd272d446,
	0x0571dc46,
	0x5e505530,
	0xc13edbcb,
	0x848a0d8d,
	0xbf2c538b,
	0xcf46e6bd,
	0x1cf09ab5,
	0xb9fcd065,
	0x8ffd462b,
	0x67628f51,
	0x2520ea93,
	0xd272d446,
	0xd272d446,
	0xe8213e80,
	0xaca12394,
	0x7f79e79a,
	0x630dad60,
	0xbebe66ff,
};
static const char ____version_ext_names[]
__used __section("__version_ext_names") =
	"misc_deregister\0"
	"__ref_stack_chk_guard\0"
	"_copy_from_user\0"
	"mutex_lock\0"
	"kfree\0"
	"mutex_unlock\0"
	"random_kmalloc_seed\0"
	"kmalloc_caches\0"
	"__kmalloc_cache_noprof\0"
	"__stack_chk_fail\0"
	"kthread_stop\0"
	"kthread_should_stop\0"
	"find_vpid\0"
	"get_pid_task\0"
	"get_task_mm\0"
	"mmput\0"
	"__put_task_struct_rcu_cb\0"
	"call_rcu\0"
	"send_sig\0"
	"msleep\0"
	"refcount_warn_saturate\0"
	"__fentry__\0"
	"__x86_return_thunk\0"
	"_printk\0"
	"misc_register\0"
	"kthread_create_on_node\0"
	"wake_up_process\0"
	"module_layout\0"
;

MODULE_INFO(depends, "");


MODULE_INFO(srcversion, "7FD5FABAABE534FD5FA7CDD");

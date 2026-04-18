/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested; /* must be set to 1 before any kill signal */
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

//Global so signal handlers can reach the supervisor context 
static supervisor_ctx_t *g_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

/* CHECK ONCE
 * Bounded Buffer — Task 3
 *
 * Circular array of LOG_BUFFER_CAPACITY (16) log_item slots.
 * One mutex + two condition variables:
 *   not_full  — producers wait here when all 16 slots are occupied
 *   not_empty — consumer waits here when 0 slots are occupied
 */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO: filled: bounded_buffer_push — producer side.
 *
 * Blocks when buffer is full (16 slots taken).
 * Returns 0 on success, -1 if shutdown begins while waiting.
 * Wakes the consumer after inserting.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->head] = *item;
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty); //wake consumer
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:bounded_buffer_pop — consumer side.
 *
 * Blocks when buffer is empty.
 * Returns 0 with item filled on success.
 * Returns -1 when shutdown is active AND buffer is fully drained
 * (tells logging_thread it is safe to exit).
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    
    //Wait while empty, but stop waiting if shutdown was signalled
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
        
    //Shutdown AND empty → consumer should exit
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->tail];
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:logging_thread — consumer thread (Task 3).
 *
 * Pops log chunks from the bounded buffer and appends them to the
 * correct per-container log file (logs/<id>.log).
 * Exits when bounded_buffer_pop returns -1 (shutdown + drained).
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    mkdir(LOG_DIR, 0755);

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open");
            continue;
        }

        size_t written = 0;
        while (written < item.length) {
            ssize_t n = write(fd, item.data + written, item.length - written);
            if (n <= 0)
                break;
            written += (size_t)n;
        }
        close(fd);
    }

    fprintf(stderr, "[supervisor] logging thread exiting cleanly\n");
    return NULL;
}

/*
 * producer_thread - one per container (Task 3).
 * Reads from the container's stdout/stderr pipe and pushes chunks
 * into the bounded buffer. Exits when the pipe closes.
 */
typedef struct {
    int pipe_read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

static void *producer_thread(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(item.container_id, 0, sizeof(item.container_id));
    strncpy(item.container_id, pa->container_id, CONTAINER_ID_LEN - 1);

    while ((n = read(pa->pipe_read_fd, item.data, LOG_CHUNK_SIZE - 1)) > 0) {
        item.length = (size_t)n;
        item.data[n] = '\0';
        bounded_buffer_push(pa->buffer, &item);
    }

    close(pa->pipe_read_fd);
    free(pa);
    return NULL;
}

/*
 * TODO:child_fn — runs INSIDE the new namespaces after clone() (Task 1)
 *
 * This function:
 *   1. Redirects stdout/stderr to the supervisor pipe
 *   2. Sets the container hostname (UTS namespace)
 *   3. chroot()s into the container rootfs
 *   4. Mounts /proc so ps/top work inside
 *   5. Applies nice value (Task 5)
 *   6. exec()s the requested command
 *
 * NOTE: clone() without CLONE_VM gives the child a copy-on-write
 * copy of the parent's address space, so cfg pointer is valid here
 * even though the parent frees its copy after clone() returns.
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    // 1. Redirect stdout and stderr to the logging pipe 
    if (cfg->log_write_fd >= 0) {
        if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0)
            perror("dup2 stdout");
        if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0)
            perror("dup2 stderr");
        close(cfg->log_write_fd);
    }

    // 2. Set hostname to container id (uses UTS namespace) 
    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    // 3. chroot into the container's rootfs 
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    // 4. Mount /proc so ps, top etc. work inside the container 
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount /proc");

    // 5. Apply nice value for Task 5 scheduling experiments 
    if (cfg->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
            perror("setpriority");
    }

    // 6. Split command string into argv[] and exec 
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, sizeof(cmd_copy) - 1);
    cmd_copy[sizeof(cmd_copy) - 1] = '\0';

    char *argv[64];
    int argc = 0;
    char *tok = strtok(cmd_copy, " \t");
    while (tok && argc < 63) {
        argv[argc++] = tok;
        tok = strtok(NULL, " \t");
    }
    argv[argc] = NULL;

    if (argc == 0) {
        fprintf(stderr, "child_fn: empty command\n");
        return 1;
    }

    execv(argv[0], argv);
    perror("execv");
    return 127;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * SIGCHLD handler - task 1 + task 2
 * reaps exited children, updates metadata,
 * classifies termination reason.
 */
static void sigchld_handler(int sig)
{
    (void)sig;
    int wstatus;
    pid_t pid;

    if (!g_ctx)
        return;

    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c && c->host_pid != pid)
            c = c->next;

        if (c) {
            /*
             * Termination classification (grading requirement):
             *   SIGKILL + stop_requested == 0  -> hard limit kill  -> KILLED
             *   stop_requested == 1            -> manual stop      -> STOPPED
             *   otherwise                      -> normal exit      -> EXITED
             */
            if (WIFSIGNALED(wstatus) && WTERMSIG(wstatus) == SIGKILL
                    && !c->stop_requested) {
                c->state = CONTAINER_KILLED;
                c->exit_signal = SIGKILL;
                fprintf(stderr, "[supervisor] container '%s' KILLED (hard limit)\n",
                        c->id);
            } else if (c->stop_requested) {
                c->state = CONTAINER_STOPPED;
                c->exit_signal = WIFSIGNALED(wstatus) ? WTERMSIG(wstatus) : 0;
                fprintf(stderr, "[supervisor] container '%s' STOPPED\n", c->id);
            } else {
                c->state = CONTAINER_EXITED;
                c->exit_code = WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : 0;
                fprintf(stderr, "[supervisor] container '%s' EXITED code=%d\n",
                        c->id, c->exit_code);
            }

            if (g_ctx->monitor_fd >= 0)
                unregister_from_monitor(g_ctx->monitor_fd, c->id, pid);
        }

        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

/* SIGTERM/SIGINT handler - tells the event loop to stop */
static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/*
 * handle_client - processes one connected CLI client inside the supervisor.
 * Called inside the supervisor's accept loop (Task 2)
 */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;

    memset(&resp, 0, sizeof(resp));

    if (read(client_fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "ERROR: bad request");
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    switch (req.kind) {

    case CMD_START:
    case CMD_RUN: {

        /* Reject duplicate running container */
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *dup = ctx->containers;
        while (dup) {
            if (strcmp(dup->id, req.container_id) == 0 &&
                    dup->state == CONTAINER_RUNNING)
                break;
            dup = dup->next;
        }
        if (dup) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: '%s' already running", req.container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        /* Create stdout/stderr pipe - container writes, supervisor reads */
        int pipefd[2];
        if (pipe(pipefd) < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: pipe: %s", strerror(errno));
            write(client_fd, &resp, sizeof(resp));
            return;
        }
        
        /*
         * child_config_t is placed on the heap.
         * clone() WITHOUT CLONE_VM gives the child a COW copy of the
         * parent's address space, so cfg is valid inside child_fn()
         * even after the parent frees it.
         */

        child_config_t *cfg = calloc(1, sizeof(child_config_t));
        if (!cfg) {
            close(pipefd[0]);
            close(pipefd[1]);
            return;
        }
        strncpy(cfg->id, req.container_id, CONTAINER_ID_LEN - 1);
        strncpy(cfg->rootfs, req.rootfs, PATH_MAX - 1);
        strncpy(cfg->command, req.command, CHILD_COMMAND_LEN - 1);
        cfg->nice_value = req.nice_value;
        cfg->log_write_fd = pipefd[1];

        /* Allocate stack for clone() - stack grows downward */
        char *stack = malloc(STACK_SIZE);
        if (!stack) {
            free(cfg);
            close(pipefd[0]);
            close(pipefd[1]);
            return;
        }
        char *stack_top = stack + STACK_SIZE;

        /*
         * clone() flags:
         *   CLONE_NEWPID - container sees itself as PID 1
         *   CLONE_NEWUTS - container has its own hostname
         *   CLONE_NEWNS  - container has its own mount namespace
         *   SIGCHLD      - parent gets SIGCHLD when child exits
         *
         * No CLONE_VM so child gets a copy-on-write copy of parent's
         * address space, which means cfg pointer is safe inside child_fn.
         */
        pid_t child_pid = clone(child_fn, stack_top,
                                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                                cfg);
        free(stack);
        free(cfg);

        /* Close write-end in supervisor - only the child writes there */
        close(pipefd[1]);

        if (child_pid < 0) {
            close(pipefd[0]);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: clone failed: %s", strerror(errno));
            write(client_fd, &resp, sizeof(resp));
            return;
        }

        /* Build container metadata record */
        container_record_t *rec = calloc(1, sizeof(container_record_t));
        if (!rec) {
            close(pipefd[0]);
            return;
        }
        strncpy(rec->id, req.container_id, CONTAINER_ID_LEN - 1);
        rec->host_pid = child_pid;
        rec->started_at = time(NULL);
        rec->state = CONTAINER_RUNNING;
        rec->soft_limit_bytes = req.soft_limit_bytes;
        rec->hard_limit_bytes = req.hard_limit_bytes;
        rec->stop_requested = 0;
        snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req.container_id);

        pthread_mutex_lock(&ctx->metadata_lock);
        rec->next = ctx->containers;
        ctx->containers = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);

        /* Start producer thread to read container output into the buffer */
        producer_arg_t *pa = calloc(1, sizeof(producer_arg_t));
        if (pa) {
            pa->pipe_read_fd = pipefd[0];
            strncpy(pa->container_id, req.container_id, CONTAINER_ID_LEN - 1);
            pa->buffer = &ctx->log_buffer;
            pthread_t pt;
            pthread_create(&pt, NULL, producer_thread, pa);
            pthread_detach(pt);
        } else {
            close(pipefd[0]);
        }

        /* Register with kernel monitor if it is loaded (Task 4) */
        if (ctx->monitor_fd >= 0) {
            if (register_with_monitor(ctx->monitor_fd,
                                      req.container_id, child_pid,
                                      req.soft_limit_bytes,
                                      req.hard_limit_bytes) < 0)
                fprintf(stderr,
                        "[supervisor] warn: register_with_monitor failed: %s\n",
                        strerror(errno));
            else
                fprintf(stderr,
                        "[supervisor] registered '%s' pid=%d with kernel monitor\n",
                        req.container_id, child_pid);
        }

        fprintf(stderr, "[supervisor] started '%s' pid=%d\n",
                req.container_id, child_pid);

        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "OK: started '%s' pid=%d", req.container_id, child_pid);
        write(client_fd, &resp, sizeof(resp));

        /*
         * CMD_RUN: block until container exits then update state.
         * CMD_START: return immediately.
         */
        if (req.kind == CMD_RUN) {
            int ws;
            waitpid(child_pid, &ws, 0);
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *r = ctx->containers;
            while (r && r->host_pid != child_pid)
                r = r->next;
            if (r) {
                if (WIFEXITED(ws)) {
                    r->state = CONTAINER_EXITED;
                    r->exit_code = WEXITSTATUS(ws);
                } else {
                    r->state = r->stop_requested
                               ? CONTAINER_STOPPED : CONTAINER_KILLED;
                    r->exit_signal = WTERMSIG(ws);
                }
                if (ctx->monitor_fd >= 0)
                    unregister_from_monitor(ctx->monitor_fd, r->id, child_pid);
            }
            pthread_mutex_unlock(&ctx->metadata_lock);
        }
        break;
    }
    
    // CMD_PS — print metadata table for all tracked container

    case CMD_PS: {
        char line[256];
        int len;

        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message), "PS output follows:");
        write(client_fd, &resp, sizeof(resp));

        len = snprintf(line, sizeof(line),
                       "%-14s %-7s %-10s %-12s %-12s %-20s\n",
                       "ID", "PID", "STATE", "SOFT(MiB)", "HARD(MiB)", "STARTED");
        write(client_fd, line, len);

        len = snprintf(line, sizeof(line),
                       "--------------------------------------------------------------\n");
        write(client_fd, line, len);

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        if (!c) {
            len = snprintf(line, sizeof(line), "(no containers)\n");
            write(client_fd, line, len);
        }
        while (c) {
            char ts[32];
            struct tm tm_info;
            localtime_r(&c->started_at, &tm_info);
            strftime(ts, sizeof(ts), "%H:%M:%S", &tm_info);

            len = snprintf(line, sizeof(line),
                           "%-14s %-7d %-10s %-12lu %-12lu %-20s\n",
                           c->id,
                           c->host_pid,
                           state_to_string(c->state),
                           c->soft_limit_bytes >> 20,
                           c->hard_limit_bytes >> 20,
                           ts);
            write(client_fd, line, len);
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        len = snprintf(line, sizeof(line), "---END---\n");
        write(client_fd, line, len);
        break;
    }
    
    // CMD_LOGS — stream per-container log file to client

    case CMD_LOGS: {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);

        FILE *f = fopen(path, "r");
        if (!f) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: no log for '%s': %s",
                     req.container_id, strerror(errno));
            write(client_fd, &resp, sizeof(resp));
            return;
        }
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message), "LOG: %s", req.container_id);
        write(client_fd, &resp, sizeof(resp));

        char buf[512];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
            write(client_fd, buf, n);
        fclose(f);
        break;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && strcmp(c->id, req.container_id) != 0)
            c = c->next;

        if (!c || c->state != CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: '%s' not running", req.container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }

        /*
         * CRITICAL: set stop_requested = 1 BEFORE sending any signal.
         * The SIGCHLD handler reads this flag to decide termination reason.
         */
        c->stop_requested = 1;
        pid_t pid = c->host_pid;
        pthread_mutex_unlock(&ctx->metadata_lock);

        kill(pid, SIGTERM);
        fprintf(stderr, "[supervisor] stop: SIGTERM -> '%s' pid=%d\n",
                req.container_id, pid);

        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "OK: SIGTERM sent to '%s' (pid=%d)", req.container_id, pid);
        write(client_fd, &resp, sizeof(resp));
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "ERROR: unknown command");
        write(client_fd, &resp, sizeof(resp));
        break;
    }
}
/*
 * TODO:run_supervisor — Task 1 + 2 + 4 + 6
 *
 * Steps:
 *   1) Try to open /dev/container_monitor (graceful if not loaded)
 *   2) Create + bind UNIX domain socket at CONTROL_PATH
 *   3) Install SIGCHLD, SIGTERM, SIGINT handlers
 *   4) Spawn logger (consumer) thread
 *   5) Event loop: accept CLI clients, dispatch to handle_client()
 *   6) On shutdown: stop containers, join logger, free memory, cleanup fds
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /*
     * TODO:
     *   1) open /dev/container_monitor
     *   2) create the control socket / FIFO / shared-memory channel
     *   3) install SIGCHLD / SIGINT / SIGTERM handling
     *   4) spawn the logger thread
     *   5) enter the supervisor event loop
     */
    fprintf(stderr, "[supervisor] starting (base-rootfs=%s)\n", rootfs);

    /* Step 1: open kernel monitor device */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr,
                "[supervisor] warn: /dev/container_monitor not available (%s)"
                " -- kernel monitoring disabled\n",
                strerror(errno));
        ctx.monitor_fd = -1;
    } else {
        fprintf(stderr, "[supervisor] kernel monitor device open\n");
    }

    /* Step 2: create UNIX domain socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    {
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("bind");
            goto cleanup;
        }
    }

    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen");
        goto cleanup;
    }
    chmod(CONTROL_PATH, 0666);
    fprintf(stderr, "[supervisor] listening on %s\n", CONTROL_PATH);

    /* Step 3: install signal handlers */
    {
        struct sigaction sa;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sigchld_handler;
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, NULL);

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sigterm_handler;
        sa.sa_flags = SA_RESTART;
        sigaction(SIGTERM, &sa, NULL);
        sigaction(SIGINT, &sa, NULL);
    }

    /* Step 4: spawn logger thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    /* Step 5: event loop */
    fprintf(stderr, "[supervisor] ready\n");
    while (!ctx.should_stop) {
        fd_set rfds;
        struct timeval tv;

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        int sel = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }
        if (sel == 0)
            continue;

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            continue;
        }
        handle_client(&ctx, client_fd);
        close(client_fd);
    }

    /* Task 6: clean teardown */
    fprintf(stderr, "[supervisor] shutting down...\n");

    /* SIGTERM all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *c = ctx.containers;
        while (c) {
            if (c->state == CONTAINER_RUNNING) {
                c->stop_requested = 1;
                kill(c->host_pid, SIGTERM);
                fprintf(stderr, "[supervisor] SIGTERM -> '%s' pid=%d\n",
                        c->id, c->host_pid);
            }
            c = c->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    sleep(3);

    /* SIGKILL any that did not exit */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *c = ctx.containers;
        while (c) {
            if (c->state == CONTAINER_RUNNING) {
                kill(c->host_pid, SIGKILL);
                fprintf(stderr, "[supervisor] SIGKILL -> '%s' pid=%d\n",
                        c->id, c->host_pid);
            }
            c = c->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Drain remaining zombies */
    {
        int s;
        while (waitpid(-1, &s, WNOHANG) > 0) {}
    }

    /* Signal logger to drain and exit, then join it */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    fprintf(stderr, "[supervisor] logger thread joined\n");

    /* Free container metadata list */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *c = ctx.containers;
        while (c) {
            container_record_t *next = c->next;
            free(c);
            c = next;
        }
        ctx.containers = NULL;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

cleanup:
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    unlink(CONTROL_PATH);
    fprintf(stderr, "[supervisor] exited cleanly\n");
    return 0;
}

/*
 * TODO filled: send_control_request
 * Connects to the supervisor's UNIX socket, sends the request struct,
 * reads back the response struct, prints the message.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr,
                "ERROR: Cannot connect to supervisor at %s\n"
                "       Is 'sudo ./engine supervisor <rootfs>' running?\n",
                CONTROL_PATH);
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(fd);
        return 1;
    }

    if (read(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        perror("read response");
        close(fd);
        return 1;
    }

    printf("%s\n", resp.message);

    /* For PS and LOGS the supervisor streams extra text after the struct */
    if (resp.status == 0 &&
        (req->kind == CMD_PS || req->kind == CMD_LOGS)) {
        char buf[512];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf) - 1)) > 0) {
            buf[n] = '\0';
            printf("%s", buf);
        }
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}


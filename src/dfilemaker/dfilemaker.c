#include "dfilemaker.h"

/** Options specified by the user. */
extern DFM_options_t DFM_user_opts;

/*
 * A table of function pointers used for core operation.
 */
extern void (*DFM_jump_table[5])(DFM_operation_t* op, \
                                   CIRCLE_handle* handle);

/**
 * The initial seeding callback for items to process on the distributed queue
 * structure. We send all of our source items to the queue here.
 */
static void DFM_add_objects(CIRCLE_handle* handle)
{
    DFM_enqueue_work_objects(handle);
}

/**
 * The process callback for items found on the distributed queue structure.
 */
static void DFM_process_objects(CIRCLE_handle* handle)
{
    char op[CIRCLE_MAX_STRING_LEN];
    /*
        const char* DFM_op_string_table[] = {
            "TREEWALK",
            "COPY",
            "CLEANUP",
            "COMPARE"
        };
    */

    /* Pop an item off the queue */
    handle->dequeue(op);
    DFM_operation_t* opt = DFM_decode_operation(op);

    /*
        BAYER_LOG(BAYER_LOG_DBG, "Performing operation `%s' on operand `%s' (`%d' remain on local queue).", \
            DFM_op_string_table[opt->code], opt->operand, handle->local_queue_size());
    */

    DFM_jump_table[opt->code](opt, handle);

    DFM_opt_free(&opt);
    return;
}

/**
 * Print the current version.
 */
static void DFM_print_version()
{
    fprintf(stdout, "%s-%s\n", PACKAGE_NAME, PACKAGE_VERSION);
}

/**
 * Print a usage message.
 */
void DFM_print_usage(char** argv)
{
    printf("Usage: dfilemaker [options] path\n");
    fflush(stdout);
}

int main(int argc, \
         char** argv)
{
    int c;
    int option_index = 0;

    MPI_Init(&argc, &argv);
    bayer_init();

    /* Initialize our processing library and related callbacks. */
    /* This is a bit of chicken-and-egg problem, because we'd like
     * to have our rank to filter output messages below but we might
     * also want to set different libcircle flags based on command line
     * options -- for now just pass in the default flags */
    DFM_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DFM_add_objects);
    CIRCLE_cb_process(&DFM_process_objects);

    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;
    bayer_debug_level = BAYER_LOG_INFO;

    static struct option long_options[] = {
        {"debug"   , required_argument, 0, 'd'},
        {"depth"   , required_argument, 0, 'e'},
        {"fill"    , required_argument, 0, 'f'},
        {"help"    , no_argument      , 0, 'h'},
        {"seed"    , required_argument, 0, 'i'},
        {"ratio"   , required_argument, 0, 'r'},
        {"size"    , required_argument, 0, 's'},
        {"version" , no_argument      , 0, 'v'},
        {"width"   , required_argument, 0, 'w'},
        {0         , 0                , 0, 0  }
    };

    /* Parse options */
    while((c = getopt_long(argc, argv, "d:e:f:hi:r:s:vw:", \
                           long_options, &option_index)) != -1) {
        switch(c) {

            case 'd':

                if(strncmp(optarg, "fatal", 5) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_FATAL;
                    bayer_debug_level = BAYER_LOG_FATAL;

                    if(DFM_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: fatal");
                    }

                }
                else if(strncmp(optarg, "err", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_ERR;
                    bayer_debug_level = BAYER_LOG_ERR;

                    if(DFM_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: errors");
                    }

                }
                else if(strncmp(optarg, "warn", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN;
                    bayer_debug_level = BAYER_LOG_WARN;

                    if(DFM_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: warnings");
                    }

                }
                else if(strncmp(optarg, "info", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN; /* we back off a level on CIRCLE verbosity */
                    bayer_debug_level = BAYER_LOG_INFO;

                    if(DFM_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: info");
                    }

                }
                else if(strncmp(optarg, "dbg", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_DBG;
                    bayer_debug_level = BAYER_LOG_DBG;

                    if(DFM_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: debug");
                    }

                }
                else {
                    if(DFM_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level `%s' not recognized. " \
                            "Defaulting to `info'.", optarg);
                    }
                }

                break;

            case 'h':

                if(DFM_global_rank == 0) {
                    DFM_print_usage(argv);
                }

                DFM_exit(EXIT_SUCCESS);
                break;

            case 'v':

                if(DFM_global_rank == 0) {
                    DFM_print_version();
                }

                DFM_exit(EXIT_SUCCESS);
                break;

            case '?':
            default:

                if(DFM_global_rank == 0) {
                    if(optopt == 'd') {
                        DFM_print_usage(argv);
                        fprintf(stderr, "Option -%c requires an argument.\n", \
                                optopt);
                    }
                    else if(isprint(optopt)) {
                        DFM_print_usage(argv);
                        fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                    }
                    else {
                        DFM_print_usage(argv);
                        fprintf(stderr,
                                "Unknown option character `\\x%x'.\n",
                                optopt);
                    }
                }

                DFM_exit(EXIT_FAILURE);
                break;
        }
    }

    /* Initialize our jump table for core operations. */
    DFM_jump_table[TREEWALK] = DFM_do_treewalk;
    DFM_jump_table[COPY]     = DFM_do_copy;
    DFM_jump_table[CLEANUP]  = DFM_do_cleanup;
    DFM_jump_table[COMPARE]  = DFM_do_compare;

    /* allocate buffer to read/write files, aligned on 1MB boundaries */
    size_t alignment = 1024*1024;
    DFM_user_opts.block_buf1 = (char*) BAYER_MEMALIGN(
        DFM_user_opts.block_size, alignment);
    DFM_user_opts.block_buf2 = (char*) BAYER_MEMALIGN(
        DFM_user_opts.block_size, alignment);

    /* Set the log level for the processing library. */
    CIRCLE_enable_logging(CIRCLE_debug);

    /* Perform the actual file copy. */
    CIRCLE_begin();

    /* Let the processing library cleanup. */
    CIRCLE_finalize();

    exit(EXIT_SUCCESS);
}

/* EOF */

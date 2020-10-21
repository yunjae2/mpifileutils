#ifndef _COMMON_H
#define _COMMON_H

struct {
    int maxdepth;
    char * root;
} options;

extern uint64_t now_secs;
extern uint64_t now_usecs;

int dfind_main(int argc, char** argv);

#undef DEBUG

#ifdef DEBUG
#define dbprintf(...) do { fprintf(stderr, __VA_ARGS__); } while(0)
#else
#define dbprintf(...) do {} while (0)
#endif

#endif

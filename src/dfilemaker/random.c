/*
 * @file Functions related to randomness.
 */

void DFM_generate_random_string(char *str, size_t len) {
    /* Section 3.276 of the Open Group Base Specification. */
    char posix_chars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789._-";

    for(int i = 0; i < len; ++i) {
        str[i] = posix_chars[rand() % (sizeof(posix_chars) - 1)];
    }

    str[len] = 0;
}

/* EOF */

#pragma once

#include "jerasure.h"
#include "reed_sol.h"
#include "utils.h"

void encode(int k, int g, int real_l, char **data, char **coding, int blocksize,
            Encode_Type encode_type);
void make_lrc_coding_matrix(int k, int g, int real_l, int *coding_matrix);
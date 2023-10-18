#include "../include/erasure_code.h"
#include <algorithm>
#include <cstring>

static void make_lrc_coding_matrix(int k, int g, int real_l,
                                   int *coding_matrix) {
  my_assert(k % real_l == 0);
  int num_of_data_block_per_group = k / real_l;

  int *matrix = reed_sol_vandermonde_coding_matrix(k, g + 1, 8);
  my_assert(matrix != nullptr && coding_matrix != nullptr);

  bzero(coding_matrix, sizeof(int) * k * (g + real_l));

  // 编码矩阵前g行是传统的范德蒙矩阵
  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      coding_matrix[i * k + j] = matrix[(i + 1) * k + j];
    }
  }

  // 编码矩阵后面real_l行用于生成局部校验块
  for (int i = 0; i < real_l; i++) {
    for (int j = 0; j < k; j++) {
      if (i * num_of_data_block_per_group <= j &&
          j < (i + 1) * num_of_data_block_per_group) {
        coding_matrix[(i + g) * k + j] = 1;
      }
    }
  }

  free(matrix);
}

void encode(int k, int g, int real_l, char **data, char **coding, int blocksize,
            Encode_Type encode_type) {
  if (encode_type == Encode_Type::Azure_LRC) {
    std::vector<int> coding_matrix(k * (g + real_l), 0);
    make_lrc_coding_matrix(k, g, real_l, coding_matrix.data());
    jerasure_matrix_encode(k, g + real_l, 8, coding_matrix.data(), data, coding,
                           blocksize);
  }
}
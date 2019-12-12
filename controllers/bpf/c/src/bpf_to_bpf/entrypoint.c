/**
 * @brief Example C-based BPF program that prints out the parameters
 * passed to it
 */
#include <morgan_interface.h>

#include "helper.h"

extern bool entrypoint(const uint8_t *input) {
  sol_log(__FILE__);
  helper_function();
  sol_log(__FILE__);
  return true;
}

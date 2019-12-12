#include <morgan_interface.h>

static const char msg[] = "This is a message";
static const char msg2[] = "This is a different message";

extern bool entrypoint(const uint8_t *input) {
  sol_log((char*)msg);
  sol_log((char*)msg2);
  return true;
}

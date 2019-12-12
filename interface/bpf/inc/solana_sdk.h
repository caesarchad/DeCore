#pragma once
/**
 * @brief Morgan C-based BPF program utility functions and types
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Pick up static_assert if C11 or greater
 *
 * Inlined here until <assert.h> is available
 */
#if (defined _ISOC11_SOURCE || (defined __STDC_VERSION__ && __STDC_VERSION__ >= 201112L)) && !defined (__cplusplus)
#undef static_assert
#define static_assert _Static_assert
#endif

/**
 * Numeric types
 */
#ifndef __LP64__
#error LP64 data model required
#endif

typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef signed short int16_t;
typedef unsigned short uint16_t;
typedef signed int int32_t;
typedef unsigned int uint32_t;
typedef signed long int int64_t;
typedef unsigned long int uint64_t;
typedef int64_t ssize_t;
typedef uint64_t size_t;

#if defined (__cplusplus) || defined(static_assert)
static_assert(sizeof(int8_t) == 1);
static_assert(sizeof(uint8_t) == 1);
static_assert(sizeof(int16_t) == 2);
static_assert(sizeof(uint16_t) == 2);
static_assert(sizeof(int32_t) == 4);
static_assert(sizeof(uint32_t) == 4);
static_assert(sizeof(int64_t) == 8);
static_assert(sizeof(uint64_t) == 8);
#endif

/**
 * NULL
 */
#define NULL 0

/**
 * Boolean type
 */
#ifndef __cplusplus
#include <stdbool.h>
#endif

/**
 * Helper function that prints a string to stdout
 */
void sol_log(const char *);

/**
 * Helper function that prints a 64 bit values represented in hexadecimal
 * to stdout
 */
void sol_log_64(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t);


/**
 * Prefix for all BPF functions
 *
 * This prefix should be used for functions in order to facilitate
 * interoperability with BPF representation
 */
#define SOL_FN_PREFIX __attribute__((always_inline)) static

/**
 * Size of Public key in bytes
 */
#define SIZE_PUBKEY 32

/**
 * Public key
 */
typedef struct {
  uint8_t x[SIZE_PUBKEY];
} SolPubkey;

/**
 * Compares two public keys
 *
 * @param one First public key
 * @param two Second public key
 * @return true if the same
 */
SOL_FN_PREFIX bool SolPubkey_same(const SolPubkey *one, const SolPubkey *two) {
  for (int i = 0; i < sizeof(*one); i++) {
    if (one->x[i] != two->x[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Keyed Account
 */
typedef struct {
  SolPubkey *key;        /** Public key of the account */
  bool is_signer;        /** Transaction was signed by this account's key */
  uint64_t *difs;      /** Number of difs owned by this account */
  uint64_t userdata_len; /** Length of data in bytes */
  uint8_t *userdata;     /** On-chain data within this account */
  SolPubkey *owner;      /** Program that owns this account */
} SolKeyedAccount;

/**
 * Copies memory
 */
SOL_FN_PREFIX void sol_memcpy(void *dst, const void *src, int len) {
  for (int i = 0; i < len; i++) {
    *((uint8_t *)dst + i) = *((const uint8_t *)src + i);
  }
}

/**
 * Compares memory
 */
SOL_FN_PREFIX int sol_memcmp(const void *s1, const void *s2, int n) {
  for (int i = 0; i < n; i++) {
    uint8_t diff = *((uint8_t *)s1 + i) - *((const uint8_t *)s2 + i);
    if (diff) {
      return diff;
    }
  }
  return 0;
}

/**
 * Fill a byte string with a byte value
 */
SOL_FN_PREFIX void *sol_memset(void *b, int c, size_t len) {
  uint8_t *a = (uint8_t *) b;
  while (len > 0) {
    *a = c;
    a++;
    len--;
  }
}

/**
 * Find length of string
 */
SOL_FN_PREFIX size_t sol_strlen(const char *s) {
  size_t len = 0;
  while (*s) {
    len++;
    s++;
  }
  return len;
}

/**
 * Computes the number of elements in an array
 */
#define SOL_ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

/**
 * Panics
 *
 * Prints the line number where the panic occurred and then causes
 * the BPF VM to immediately halt execution. No accounts' userdata are updated
 */
#define sol_panic() _sol_panic(__LINE__)
SOL_FN_PREFIX void _sol_panic(uint64_t line) {
  sol_log_64(0xFF, 0xFF, 0xFF, 0xFF, line);
  uint8_t *pv = (uint8_t *)1;
  *pv = 1;
}

/**
 * Asserts
 */
#define sol_assert(expr)  \
if (!(expr)) {          \
  _sol_panic(__LINE__); \
}

/**
 * Structure that the program's entrypoint input data is deserialized into.
 */
typedef struct {
  SolKeyedAccount* ka; /** Pointer to an array of SolKeyedAccount, must already
                           point to an array of SolKeyedAccounts */
  uint64_t ka_num; /** Number of SolKeyedAccount entries in `ka` */
  const uint8_t *data; /** pointer to the instruction data */
  uint64_t data_len; /** Length in bytes of the instruction data */
  uint64_t tick_height; /** Current ledger tick */
  const SolPubkey *program_id; /** program_id of the currently executing program */
} SolParameters;

/**
 * De-serializes the input parameters into usable types
 *
 * Use this function to deserialize the buffer passed to the program entrypoint
 * into usable types.  This function does not perform copy deserialization,
 * instead it populates the pointers and lengths in SolKeyedAccount and data so
 * that any modification to difs or account data take place on the original
 * buffer.  Doing so also eliminates the need to serialize back into the buffer
 * at program end.
 *
 * @param input Source buffer containing serialized input parameters
 * @param params Pointer to a SolParameters structure
 * @return Boolean true if successful.
 */
SOL_FN_PREFIX bool sol_deserialize(
  const uint8_t *input,
  SolParameters *params,
  uint64_t ka_num
) {
  if (NULL == input || NULL == params) {
    return false;
  }
  params->ka_num = *(uint64_t *) input;
  if (ka_num < *(uint64_t *) input) {
    return false;
  }

  input += sizeof(uint64_t);
  for (int i = 0; i < params->ka_num; i++) {
    // key
    params->ka[i].is_signer = *(uint64_t *) input != 0;
    input += sizeof(uint64_t);
    params->ka[i].key = (SolPubkey *) input;
    input += sizeof(SolPubkey);

    // difs
    params->ka[i].difs = (uint64_t *) input;
    input += sizeof(uint64_t);

    // account userdata
    params->ka[i].userdata_len = *(uint64_t *) input;
    input += sizeof(uint64_t);
    params->ka[i].userdata = (uint8_t *) input;
    input += params->ka[i].userdata_len;

    // owner
    params->ka[i].owner = (SolPubkey *) input;
    input += sizeof(SolPubkey);
  }

  params->data_len = *(uint64_t *) input;
  input += sizeof(uint64_t);
  params->data = input;
  input += params->data_len;

  params->tick_height = *(uint64_t *) input;
  input += sizeof(uint64_t);
  params->program_id = (SolPubkey *) input;
  input += sizeof(SolPubkey);

  return true;
}

/**
 * Debugging utilities
 * @{
 */

/**
 * Prints the hexadecimal representation of a public key
 *
 * @param key The public key to print
 */
SOL_FN_PREFIX void sol_log_key(const SolPubkey *key) {
  for (int j = 0; j < sizeof(*key); j++) {
    sol_log_64(0, 0, 0, j, key->x[j]);
  }
}

/**
 * Prints the hexadecimal representation of an array
 *
 * @param array The array to print
 */
SOL_FN_PREFIX void sol_log_array(const uint8_t *array, int len) {
  for (int j = 0; j < len; j++) {
    sol_log_64(0, 0, 0, j, array[j]);
  }
}

/**
 * Prints the program's input parameters
 *
 * @param params Pointer to a SolParameters structure
 */
SOL_FN_PREFIX void sol_log_params(const SolParameters *params) {
  sol_log("- Tick height:");
  sol_log_64(params->tick_height, 0, 0, 0, 0);
  sol_log("- Program identifier:");
  sol_log_key(params->program_id);

  sol_log("- Number of KeyedAccounts");
  sol_log_64(0, 0, 0, 0, params->ka_num);
  for (int i = 0; i < params->ka_num; i++) {
    sol_log("  - Is signer");
    sol_log_64(0, 0, 0, 0, params->ka[i].is_signer);
    sol_log("  - Key");
    sol_log_key(params->ka[i].key);
    sol_log("  - Difs");
    sol_log_64(0, 0, 0, 0, *params->ka[i].difs);
    sol_log("  - Userdata");
    sol_log_array(params->ka[i].userdata, params->ka[i].userdata_len);
    sol_log("  - Owner");
    sol_log_key(params->ka[i].owner);
  }
  sol_log("- Instruction data\0");
  sol_log_array(params->data, params->data_len);
}

/**@}*/

/**
 * Program instruction entrypoint
 *
 * @param input Buffer of serialized input parameters.  Use sol_deserialize() to decode
 * @return true if the instruction executed successfully
 */
bool entrypoint(const uint8_t *input);


#ifdef SOL_TEST
/**
 * Stub log functions when building tests
 */
#include <stdio.h>
void sol_log(const char *s) {
  printf("sol_log: %s\n", s);
}
void sol_log_64(uint64_t arg1, uint64_t arg2, uint64_t arg3, uint64_t arg4, uint64_t arg5) {
  printf("sol_log_64: %llu, %llu, %llu, %llu, %llu\n", arg1, arg2, arg3, arg4, arg5);
}
#endif

#ifdef __cplusplus
}
#endif

/**@}*/

#include "os.h"
#include "tkey.h"

#define MAX_DIGITS 50
// static int i, j = 0;
// static char buffer[1024];

//#define KEY_RSA_1 1140359513L
//#define KEY_RSA_2 257
//#define KEY_RSA_3 1140359513L
//#define KEY_RSA_4 847451393L
//#define KEY_RSA_5 619132369L
//#define KEY_RSA_6 257
//#define KEY_RSA_7 619132369L
//#define KEY_RSA_8 484185221L

struct public_key_class {
  int64_t modulus;
  int64_t exponent;
};

struct private_key_class {
  int64_t modulus;
  int64_t exponent;
};

// This should totally be in the math library.
int64_t gcdfunc(int64_t a, int64_t b) {
  int64_t c;
  while (a != 0) {
    c = a;
    a = b % a;
    b = c;
  }
  return b;
}

int64_t ExtEuclid(int64_t a, int64_t b) {
  int64_t x = 0;
  int64_t y = 1;
  int64_t u = 1;
  int64_t v = 0;
  int64_t gcd = b;
  int64_t m, n, q, r;
  while (a != 0) {
    q = gcd / a;
    r = gcd % a;
    m = x - u * q;
    n = y - v * q;
    gcd = a;
    a = r;
    x = u;
    y = v;
    u = m;
    v = n;
  }
  return y;
}

int64_t rsa_modExp(int64_t b, int64_t e, int64_t m) {
  if (b < 0 || e < 0 || m <= 0) {
    exit(1);
  }
  b = b % m;
  if (e == 0) return 1;
  if (e == 1) return b;
  if (e % 2 == 0) {
    return (rsa_modExp(b * b % m, e / 2, m) % m);
  }
  if (e % 2 == 1) {
    return (b * rsa_modExp(b, (e - 1), m) % m);
  }

  return 0;
}

int64_t *rsa_encrypt(const char *message, const uint64_t message_size, const struct public_key_class *pub) {
  int64_t *encrypted = malloc(sizeof(int64_t) * message_size);
  if (encrypted == NULL) {
    fprintf(stderr, "Error: Heap allocation failed.\n");
    return NULL;
  }
  int64_t i = 0;
  for (i = 0; i < message_size; i++) {
    encrypted[i] = rsa_modExp(message[i], pub->exponent, pub->modulus);
  }
  return encrypted;
}

char *rsa_decrypt(const int64_t *message, const uint64_t message_size, const struct private_key_class *priv) {
  if (message_size % sizeof(int64_t) != 0) {
    fprintf(stderr,
            "Error: message_size is not divisible by %d, so cannot be output "
            "of rsa_encrypt\n",
            (int)sizeof(int64_t));
    return NULL;
  }
  // We allocate space to do the decryption (temp) and space for the output as a
  // char array
  // (decrypted)
  char *decrypted = malloc(message_size / sizeof(int64_t));
  char *temp = malloc(message_size);
  if ((decrypted == NULL) || (temp == NULL)) {
    tfree(decrypted);
    tfree(temp);
    fprintf(stderr, "Error: Heap allocation failed.\n");
    return NULL;
  }
  // Now we go through each 8-byte chunk and decrypt it.
  int64_t i = 0;
  for (i = 0; i < message_size / 8; i++) {
    temp[i] = rsa_modExp(message[i], priv->exponent, priv->modulus);
  }
  // The result should be a number in the char range, which gives back the
  // original byte.
  // We put that into decrypted, then return.
  for (i = 0; i < message_size / 8; i++) {
    decrypted[i] = temp[i];
  }
  free(temp);
  return decrypted;
}

char *taosRsaEncode(int64_t modulus, int64_t exponent, char *src, int len) {
  struct public_key_class pub[1];
  pub->modulus = modulus;
  pub->exponent = exponent;

  int64_t *encrypted = rsa_encrypt(src, len, pub);
  if (!encrypted) {
    return NULL;
  }

  return (char *)encrypted;
}

char *taosRsaDecode(int64_t modulus, int64_t exponent, char *src, int len) {
  struct private_key_class priv[1];
  priv->modulus = modulus;
  priv->exponent = exponent;

  char *decrypted = rsa_decrypt((const int64_t *)src, len, priv);
  if (!decrypted) {
    return NULL;
  }

  return decrypted;
}
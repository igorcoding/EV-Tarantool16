#ifndef _AUTH_H_
#define _AUTH_H_

#include <string.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <stdint.h>
#include <assert.h>

static inline size_t calc_decode_len(const char *b64message, size_t len) { //Calculates the length of a decoded string
	size_t padding = 0;

	if (b64message[len-1] == '=') {
		if (b64message[len-2] == '=') { //last two chars are =
			padding = 2;
		} else {
			padding = 1;
		}
	}

	return (len * 3) / 4 - padding;
}

static inline int base64_decode(const char *b64_begin, const char *b64_end, unsigned char **buffer, size_t *length) { //Decodes a base64 encoded string
	assert(b64_end > b64_begin);

	int status = 0;
	BIO *bio, *b64;

	int b64_len = (int) (b64_end - b64_begin);
	int decodeLen = calc_decode_len(b64_begin, b64_len);
	*buffer = (unsigned char*) safemalloc(decodeLen);

	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new_mem_buf(b64_begin, b64_len);
	bio = BIO_push(b64, bio);

	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL); //Do not use newlines to flush buffer
	*length = BIO_read(bio, *buffer, b64_len);
	status = *length != decodeLen; //length should equal decodeLen, else something went horribly wrong
	BIO_free_all(b64);

	return status;
}

#define sha1_encode(input_begin, sz, output) STMT_START {\
	SHA1(input_begin, sz, output);\
} STMT_END


static inline int auth_autheticate() {

}

#endif // _AUTH_H_

#define _SPH_CONVERT_MAIN_

#include "sph2pipe_shorten_compat.h"
#include "../vendor/sph2pipe/ulaw.h"

#include <limits.h>

static char native_order_buf[3] = {0, 0, 0};

static void init_native_order(void) {
  if (native_order_buf[0] != 0) return;
  union {
    char ch[2];
    short int i2;
  } short_order;
  short_order.i2 = 1;
  // "01" means little-endian, "10" means big-endian (matches sph2pipe conventions)
  native_order_buf[0] = (short_order.ch[0]) ? '0' : '1';
  native_order_buf[1] = (short_order.ch[0]) ? '1' : '0';
  native_order_buf[2] = '\0';
}

// Minimal mu-law encoder (G.711), adapted from common reference implementations.
uchar pcm2ulaw(short int sample) {
  const int BIAS = 0x84;
  const int CLIP = 32635;
  static int exp_lut[256] = {0,0,1,1,2,2,2,2,3,3,3,3,3,3,3,3,
                             4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,
                             5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,
                             5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,
                             6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
                             6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
                             6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
                             6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                             7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7};

  int sign = (sample >> 8) & 0x80;
  if (sign != 0) sample = -sample;
  if (sample > CLIP) sample = CLIP;
  sample = (short int)(sample + BIAS);
  int exponent = exp_lut[(sample >> 7) & 0xFF];
  int mantissa = (sample >> (exponent + 3)) & 0x0F;
  return (uchar) ~(sign | (exponent << 4) | mantissa);
}

// Minimal A-law encoder (G.711), adapted from common reference implementations.
uchar pcm2alaw(short int pcmval) {
  int mask;
  int seg;
  uchar aval;
  static int seg_end[8] = {0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF};

  if (pcmval >= 0) {
    mask = 0xD5;
  } else {
    mask = 0x55;
    pcmval = -pcmval - 1;
    if (pcmval < 0) pcmval = 0;
  }

  // Convert from 16-bit linear to A-law.
  pcmval >>= 4;
  for (seg = 0; seg < 8; seg++) {
    if (pcmval <= seg_end[seg]) break;
  }
  if (seg >= 8) {
    return (uchar)(0x7F ^ mask);
  }
  aval = (uchar) seg << 4;
  if (seg < 2) {
    aval |= (uchar)((pcmval >> 1) & 0x0F);
  } else {
    aval |= (uchar)((pcmval >> seg) & 0x0F);
  }
  return (uchar)(aval ^ mask);
}

// Decode Shorten-compressed stream inside a SPHERE file into raw PCM16LE.
// `header_bytes` is the SPHERE header length (multiple of 1024).
int litdata_sph_shorten_to_pcm16le(const char *sph_path, long header_bytes, const char *pcm_path) {
  if (sph_path == NULL || pcm_path == NULL || header_bytes < 0) return 1;

  init_native_order();

  inpname = (char *)sph_path;
  outname = (char *)pcm_path;
  nativorder = native_order_buf;
  outorder = (char *)"01"; // little-endian PCM

  // Output settings: full duration, all channels, PCM16.
  startout = 0;
  endout = INT_MAX;
  typeout = PCM;
  sizeout = 2;
  chanout = 2;
  debug = 0;

  fpin = fopen(sph_path, "rb");
  if (fpin == NULL) return 1;
  if (fseek(fpin, header_bytes, SEEK_SET) != 0) {
    fclose(fpin);
    return 1;
  }

  fpout = fopen(pcm_path, "wb");
  if (fpout == NULL) {
    fclose(fpin);
    return 1;
  }

  int rc = shortenXtract();
  fclose(fpin);
  fclose(fpout);
  fpin = NULL;
  fpout = NULL;
  return rc;
}


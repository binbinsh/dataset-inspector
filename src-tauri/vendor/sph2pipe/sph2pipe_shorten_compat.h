// Minimal compatibility layer for the vendored sph2pipe shorten decoder.
//
// We intentionally avoid vendoring the full sph2pipe conversion pipeline.
// Only `shorten_x.c` is compiled to decode Shorten-compressed SPHERE payloads
// into raw PCM samples, and we wrap it from Rust.

#pragma once

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#endif

#ifdef _SPH_CONVERT_MAIN_
#define GLOBAL
#else
#define GLOBAL extern
#endif

// Pseudo-typedefs used by the sph2pipe sources.
#undef uchar
#define uchar unsigned char
#undef schar
#define schar signed char
#undef ushort
#define ushort unsigned short
#undef ulong
#define ulong unsigned long

// Match sph2pipe's sample type constants.
#define PCM 2
#define ULAW 1
#define ALAW 5

// Globals referenced by shorten_x.c.
GLOBAL int chanout, typeout, sizeout, startout, endout, debug;
GLOBAL char *nativorder, *outorder;
GLOBAL FILE *fpin, *fpout;
GLOBAL char *inpname, *outname;

// Tables declared in ulaw.h. We compile them once in our wrapper TU.
GLOBAL short int ulaw2pcm[256];
GLOBAL short int alaw2pcm[256];

// Entry point provided by shorten_x.c.
int shortenXtract(void);

// These are referenced from shorten_x.c even if we force PCM output.
uchar pcm2ulaw(short int sample);
uchar pcm2alaw(short int pcmval);


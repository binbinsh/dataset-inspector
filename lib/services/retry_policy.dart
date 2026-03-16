class RetryPolicy {
  RetryPolicy({
    required this.maxAttempts,
    this.backoff = defaultRetryBackoff,
  }) : assert(maxAttempts > 0);

  final int maxAttempts;
  final Duration Function(int attempt) backoff;

  Future<T> run<T>({
    required Future<T> Function(int attempt) action,
    required bool Function(Object error) shouldRetry,
  }) async {
    Object? lastError;
    StackTrace? lastStack;
    for (var attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        return await action(attempt);
      } catch (error, stackTrace) {
        final retryable = attempt < maxAttempts && shouldRetry(error);
        if (!retryable) {
          Error.throwWithStackTrace(error, stackTrace);
        }
        lastError = error;
        lastStack = stackTrace;
        await Future<void>.delayed(backoff(attempt));
      }
    }
    final error = lastError;
    if (error != null) {
      Error.throwWithStackTrace(error, lastStack ?? StackTrace.current);
    }
    throw StateError('RetryPolicy exhausted without captured error.');
  }
}

Duration defaultRetryBackoff(int attempt) {
  final safeAttempt = attempt < 1 ? 1 : attempt;
  return Duration(milliseconds: 120 * safeAttempt);
}

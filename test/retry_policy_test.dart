import 'package:dataset_inspector/services/retry_policy.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('retries until success', () async {
    final policy = RetryPolicy(maxAttempts: 3);
    var attempts = 0;
    final result = await policy.run<int>(
      action: (_) async {
        attempts += 1;
        if (attempts < 3) {
          throw const FormatException('transient');
        }
        return 42;
      },
      shouldRetry: (_) => true,
    );
    expect(result, equals(42));
    expect(attempts, equals(3));
  });

  test('stops on non-retryable error', () async {
    final policy = RetryPolicy(maxAttempts: 5);
    var attempts = 0;
    await expectLater(
      () => policy.run<void>(
        action: (_) async {
          attempts += 1;
          throw StateError('fatal');
        },
        shouldRetry: (_) => false,
      ),
      throwsA(isA<StateError>()),
    );
    expect(attempts, equals(1));
  });
}

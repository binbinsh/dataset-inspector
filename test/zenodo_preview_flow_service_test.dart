import 'package:dataset_inspector/services/zenodo_preview_flow_service.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('detectContainer recognizes zip/tar/plain', () {
    const service = ZenodoPreviewFlowService();

    expect(service.detectContainer('a.zip'), ZenodoContainerKind.zip);
    expect(service.detectContainer('a.tar'), ZenodoContainerKind.tar);
    expect(service.detectContainer('a.tar.gz'), ZenodoContainerKind.tar);
    expect(service.detectContainer('a.tgz'), ZenodoContainerKind.tar);
    expect(service.detectContainer('a.tar.zst'), ZenodoContainerKind.tar);
    expect(service.detectContainer('a.tar.zstd'), ZenodoContainerKind.tar);
    expect(service.detectContainer('a.bin'), ZenodoContainerKind.plain);
  });

  test('isInlineMediaExt matches configured media extensions', () {
    const service = ZenodoPreviewFlowService();

    expect(service.isInlineMediaExt('image.JPG'), isTrue);
    expect(service.isInlineMediaExt('audio.wav'), isTrue);
    expect(service.isInlineMediaExt('archive.tar'), isFalse);
    expect(service.isInlineMediaExt('readme.txt'), isFalse);
  });
}

// This is a basic Flutter widget test.
//
// To perform an interaction with a widget in your test, use the WidgetTester
// utility in the flutter_test package. For example, you can send tap and scroll
// gestures. You can also use WidgetTester to find child widgets in the widget
// tree, read text, and verify that the values of widget properties are correct.

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:provider/provider.dart';

import 'package:dataset_inspector/state/viewer_state.dart';
import 'package:dataset_inspector/widgets/inspector_screen.dart';

void main() {
  testWidgets('Inspector screen renders', (WidgetTester tester) async {
    final state = ViewerState();
    await tester.pumpWidget(
      ChangeNotifierProvider.value(
        value: state,
        child: const MaterialApp(
          home: InspectorScreen(),
        ),
      ),
    );

    expect(find.text('Dataset Inspector'), findsOneWidget);
  });
}

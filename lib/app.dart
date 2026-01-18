import 'package:flutter/material.dart';
import 'package:provider/provider.dart';

import 'services/app_menu_bridge.dart';
import 'state/viewer_state.dart';
import 'widgets/inspector_screen.dart';

class DatasetInspectorApp extends StatelessWidget {
  const DatasetInspectorApp({super.key});

  @override
  Widget build(BuildContext context) {
    final baseScheme = ColorScheme.fromSeed(
      seedColor: const Color(0xFF0C6B5A),
      brightness: Brightness.light,
    ).copyWith(
      primary: const Color(0xFF0B5D4C),
      secondary: const Color(0xFFE08C3C),
      tertiary: const Color(0xFF2E6F95),
      surface: const Color(0xFFFFFAF3),
      surfaceContainerHighest: const Color(0xFFF1E8DA),
      surfaceContainerHigh: const Color(0xFFF7EFE3),
      surfaceContainer: const Color(0xFFF9F2E8),
      surfaceContainerLow: const Color(0xFFFBF6EE),
      surfaceContainerLowest: const Color(0xFFFFFDF9),
      onPrimary: const Color(0xFFF4F7F2),
      onSecondary: const Color(0xFF2B1C0E),
      onSurface: const Color(0xFF1C1914),
      outline: const Color(0xFFE3D8C8),
      outlineVariant: const Color(0xFFECE3D6),
      error: const Color(0xFFB42318),
      onError: const Color(0xFFFFFFFF),
    );

    final baseText = ThemeData.light().textTheme;
    final textTheme = baseText.apply(
      fontFamily: 'SpaceGrotesk',
      displayColor: baseScheme.onSurface,
      bodyColor: baseScheme.onSurface,
    );

    final theme = ThemeData(
      colorScheme: baseScheme,
      scaffoldBackgroundColor: baseScheme.surfaceContainerLowest,
      fontFamily: 'SpaceGrotesk',
      textTheme: textTheme,
      useMaterial3: true,
      dividerColor: baseScheme.outlineVariant,
      visualDensity: VisualDensity.standard,
      chipTheme: ChipThemeData(
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(20)),
        backgroundColor: baseScheme.surfaceContainerHighest,
        labelStyle: textTheme.labelSmall?.copyWith(fontWeight: FontWeight.w600),
      ),
      inputDecorationTheme: InputDecorationTheme(
        filled: true,
        fillColor: baseScheme.surfaceContainerLow,
        hintStyle: textTheme.bodySmall?.copyWith(color: baseScheme.onSurface.withOpacity(0.5)),
        border: OutlineInputBorder(
          borderRadius: BorderRadius.circular(14),
          borderSide: BorderSide(color: baseScheme.outlineVariant),
        ),
        enabledBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(14),
          borderSide: BorderSide(color: baseScheme.outlineVariant),
        ),
        focusedBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(14),
          borderSide: BorderSide(color: baseScheme.primary, width: 1.4),
        ),
      ),
      filledButtonTheme: FilledButtonThemeData(
        style: FilledButton.styleFrom(
          textStyle: textTheme.labelLarge?.copyWith(fontWeight: FontWeight.w600),
        ),
      ),
      outlinedButtonTheme: OutlinedButtonThemeData(
        style: OutlinedButton.styleFrom(
          textStyle: textTheme.labelLarge?.copyWith(fontWeight: FontWeight.w600),
          side: BorderSide(color: baseScheme.outline),
        ),
      ),
      textButtonTheme: TextButtonThemeData(
        style: TextButton.styleFrom(
          textStyle: textTheme.labelLarge?.copyWith(fontWeight: FontWeight.w600),
          foregroundColor: baseScheme.primary,
        ),
      ),
    );

    return ChangeNotifierProvider(
      create: (_) => ViewerState()..bootstrap(),
      child: MaterialApp(
        title: 'Dataset Inspector',
        theme: theme,
        home: const InspectorScreen(),
        debugShowCheckedModeBanner: false,
        navigatorKey: AppMenuBridge.navigatorKey,
        scaffoldMessengerKey: AppMenuBridge.messengerKey,
      ),
    );
  }
}

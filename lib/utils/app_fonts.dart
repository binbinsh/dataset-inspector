import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';

class AppFonts {
  static final TextStyle sans = GoogleFonts.googleSans();
  static final TextStyle flex = GoogleFonts.getFont('Google Sans Flex');
  static final TextStyle code = GoogleFonts.googleSansCode();

  static String get sansFamily => sans.fontFamily ?? 'Google Sans';
  static String get flexFamily => flex.fontFamily ?? 'Google Sans Flex';
  static String get codeFamily => code.fontFamily ?? 'Google Sans Code';

  static TextTheme applyTitleFont(TextTheme base) {
    final family = flexFamily;
    return base.copyWith(
      displayLarge: base.displayLarge?.copyWith(fontFamily: family),
      displayMedium: base.displayMedium?.copyWith(fontFamily: family),
      displaySmall: base.displaySmall?.copyWith(fontFamily: family),
      headlineLarge: base.headlineLarge?.copyWith(fontFamily: family),
      headlineMedium: base.headlineMedium?.copyWith(fontFamily: family),
      headlineSmall: base.headlineSmall?.copyWith(fontFamily: family),
      titleLarge: base.titleLarge?.copyWith(fontFamily: family),
      titleMedium: base.titleMedium?.copyWith(fontFamily: family),
      titleSmall: base.titleSmall?.copyWith(fontFamily: family),
    );
  }

  static TextStyle flexTextStyle(TextStyle? base) {
    return (base ?? const TextStyle()).copyWith(fontFamily: flexFamily);
  }

  static TextStyle codeTextStyle({TextStyle? base, double? height}) {
    final style = GoogleFonts.googleSansCode(textStyle: base);
    return height == null ? style : style.copyWith(height: height);
  }
}

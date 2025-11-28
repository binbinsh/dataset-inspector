import nextConfig from "eslint-config-next";

const config = [
  ...nextConfig,
  {
    ignores: ["src-tauri/target/**", "src-tauri/gen/**"],
  },
];

export default config;

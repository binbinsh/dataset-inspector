import React from "react";
import { createRoot } from "react-dom/client";
import { HeroUIProvider } from "@heroui/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";

import "@/styles/app.css";
import { createQueryClient } from "@/lib/query-client";
import { router } from "@/router";
import { RouterProvider } from "@tanstack/react-router";

const queryClient = createQueryClient();

const root = document.getElementById("root");
if (!root) throw new Error("Missing #root element.");

createRoot(root).render(
  <React.StrictMode>
    <HeroUIProvider>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
        {import.meta.env.DEV ? <ReactQueryDevtools initialIsOpen={false} /> : null}
      </QueryClientProvider>
    </HeroUIProvider>
  </React.StrictMode>,
);

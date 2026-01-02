import * as React from "react";
import { cn } from "@/lib/utils";
import { X } from "lucide-react";

export interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  startContent?: React.ReactNode;
  endContent?: React.ReactNode;
  isClearable?: boolean;
  onClear?: () => void;
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type, startContent, endContent, isClearable, onClear, value, onChange, ...props }, ref) => {
    const showClear = isClearable && value && String(value).length > 0;

    const handleClear = () => {
      if (onClear) {
        onClear();
      } else if (onChange) {
        const event = {
          target: { value: "" },
        } as React.ChangeEvent<HTMLInputElement>;
        onChange(event);
      }
    };

    return (
      <div className="relative flex items-center">
        {startContent && (
          <div className="absolute left-3 flex items-center pointer-events-none text-slate-500">
            {startContent}
          </div>
        )}
        <input
          type={type}
          className={cn(
            "flex h-10 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm ring-offset-white file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-slate-500 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-500 focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50",
            startContent && "pl-10",
            (endContent || showClear) && "pr-10",
            className
          )}
          ref={ref}
          value={value}
          onChange={onChange}
          {...props}
        />
        {(showClear || endContent) && (
          <div className="absolute right-3 flex items-center gap-1">
            {showClear && (
              <button
                type="button"
                onClick={handleClear}
                className="text-slate-400 hover:text-slate-600 transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            )}
            {endContent}
          </div>
        )}
      </div>
    );
  }
);
Input.displayName = "Input";

export { Input };

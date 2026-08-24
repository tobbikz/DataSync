import { LoginForm } from "@/components/LoginForm";

export default function LoginPage() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4">
      <div className="w-full max-w-[360px]">
        <div className="panel overflow-hidden">
          <div className="panel-header">
            <div className="flex items-center gap-2.5">
              <div className="flex size-7 items-center justify-center bg-accent text-[11px] font-bold text-white">
                DS
              </div>
              <div>
                <p className="text-[13px] font-semibold text-foreground">
                  DataSync
                </p>
                <p className="text-[11px] text-foreground-muted">
                  Control Plane
                </p>
              </div>
            </div>
            <span className="tag text-foreground-muted">
              v0.1
            </span>
          </div>

          <div className="panel-body">
            <p className="mb-5 text-[13px] text-foreground-secondary">
              Sign in to access the operations console.
            </p>
            <LoginForm />
          </div>
        </div>

        <p className="mt-4 text-center font-mono text-[11px] text-foreground-muted">
          dev: admin / datasync
        </p>
      </div>
    </div>
  );
}

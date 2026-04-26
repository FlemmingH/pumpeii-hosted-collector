const shellStyle = {
  minHeight: "100vh",
  display: "grid",
  placeItems: "center",
  background:
    "radial-gradient(circle at top, rgba(23, 37, 84, 0.12), transparent 45%), #f7f7f2",
  color: "#111827",
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace',
  padding: "2rem",
} satisfies React.CSSProperties;

const cardStyle = {
  maxWidth: "44rem",
  border: "1px solid rgba(17, 24, 39, 0.12)",
  borderRadius: "1rem",
  padding: "1.5rem",
  background: "rgba(255, 255, 255, 0.88)",
  boxShadow: "0 20px 60px rgba(15, 23, 42, 0.08)",
} satisfies React.CSSProperties;

export default function HomePage() {
  return (
    <main style={shellStyle}>
      <section style={cardStyle}>
        <p style={{ margin: 0, fontSize: "0.8rem", letterSpacing: "0.08em", textTransform: "uppercase" }}>
          Pumpeii Hosted Collector
        </p>
        <h1 style={{ margin: "0.75rem 0 0", fontSize: "2rem", lineHeight: 1.1 }}>
          Coinalyze rolling liquidation capture
        </h1>
        <p style={{ margin: "1rem 0 0", lineHeight: 1.6 }}>
          This app is a server-only Phase 1 collector for Vercel and Supabase. Use the cron route to
          refresh the rolling overlap window and the health route to confirm the deployment is live.
        </p>
      </section>
    </main>
  );
}
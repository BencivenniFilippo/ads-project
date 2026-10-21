import sys
import pandas as pd
import matplotlib.pyplot as plt

trace_file = sys.argv[1]
df = pd.read_csv(trace_file)

ok_df = df[df["status"] == "ok"]
err_df = df[df["status"] == "error"]

plt.figure(figsize=(11, 5))

# Latencia de cada solicitud exitosa y media móvil
plt.scatter(ok_df["t_send_s"], ok_df["latency_ms"], s=10, alpha=0.4, label="Latencia individual (OK)")
plt.plot(
    ok_df["t_send_s"],
    ok_df["latency_ms"].rolling(window=25, min_periods=1).mean(),
    color="navy",
    linewidth=2,
    label="Media móvil (25 reqs)",
)

# Solicitudes fallidas (si las hubiera)
if not err_df.empty:
    plt.scatter(
        err_df["t_send_s"],
        err_df["latency_ms"],
        color="red",
        marker="x",
        s=40,
        label="Errores",
    )

plt.xlabel("Tiempo desde el inicio del experimento (s)")
plt.ylabel("Latencia (ms)")
plt.title("Latencia por solicitud durante la caída de un servidor")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig(trace_file.replace(".csv", ".png"), dpi=150)
print(f"Gráfica guardada en {trace_file.replace('.csv', '.png')}")
import os, math
try:
    import joblib
except ImportError:
    joblib = None

class AISignal:
    def __init__(self, model_path="ai_model.pkl"):
        self.model = None
        if joblib and os.path.exists(model_path):
            try:
                self.model = joblib.load(model_path)
            except Exception:
                self.model = None

    def predict_proba(self, feat: dict) -> float:
        # 고정 순서 특성 벡터
        keys = ("msi","rsi","bbw","atrp","spike_mult","imbalance","aggr_ratio",
                "spread_pct","hour","ret_30s","ret_2m","vwap_gap")
        x = [float(feat.get(k, 0.0) or 0.0) for k in keys]

        if self.model is None:
            # 휴리스틱 폴백(대략적인 가중치 스코어 → 0~1 확률)
            s = 0.0
            s += 0.15 * min(max(feat.get("msi",5)/10.0, 0), 1)
            s += 0.20 * min(max((feat.get("rsi",50)-50)/50, 0), 1)
            s += 0.20 * min(max(feat.get("spike_mult",0)/3.0, 0), 1)
            s += 0.15 * min(max(1.5 - feat.get("imbalance",1.0), 0), 1)
            s += 0.10 * min(max(1.2 - (feat.get("spread_pct",0)/0.01), 0), 1)
            s += 0.20 * min(max((feat.get("aggr_ratio",1.0)-1.0)/2.0, 0), 1)
            return max(0.0, min(1.0, 0.5 + (s - 0.35)))
        else:
            import numpy as np
            return float(self.model.predict_proba(np.array([x]))[0,1])

def extract_features(*, msi, price, vwap, rsi, bbw, atrp, spike_mult,
                     imbalance, aggr_ratio, spread_pct, hour, ret_30s, ret_2m):
    vwap_gap = 0.0 if not vwap else (price / vwap - 1.0)
    return {
        "msi": msi, "rsi": rsi, "bbw": bbw, "atrp": atrp,
        "spike_mult": spike_mult, "imbalance": imbalance, "aggr_ratio": aggr_ratio,
        "spread_pct": spread_pct, "hour": float(hour),
        "ret_30s": ret_30s, "ret_2m": ret_2m,
        "vwap_gap": vwap_gap,
    }
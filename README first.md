# ⏳ Event Matcher — Greedy Forward-AsOf Join

A simple Python utility for time-based event matching — perfect for analytics, behavior tracking, or attribution modeling.

## 🚀 What it does
This tool helps you **connect two events in time** for the same user:
- e.g. “Match each support ticket with the next payment within 24 hours.”

It supports:
- ✅ `forward_asof_match()` — quick matching (many-to-one possible)
- ✅ `forward_asof_greedy_one_to_one()` — ensures strict one-to-one pairing

---

## 🧠 Use Cases
- **Fintech:** Link support tickets to payments  
- **E-commerce:** Match product views to purchases  
- **SaaS:** Track trial signups to first upgrade  
- **EdTech:** Connect student logins to lesson completions  

---

## ⚙️ Quick Example

```python
from event_matcher import forward_asof_greedy_one_to_one
import pandas as pd

tickets = pd.DataFrame({
    "user_id": ["u1", "u1", "u2"],
    "interaction_time": ["2025-11-01 09:00", "2025-11-01 10:00", "2025-11-01 12:00"]
})

payments = pd.DataFrame({
    "user_id": ["u1", "u2"],
    "payment_time": ["2025-11-01 09:30", "2025-11-01 13:00"]
})

out = forward_asof_greedy_one_to_one(
    tickets, payments,
    user_col="user_id",
    t_time_col="interaction_time",
    p_time_col="payment_time"
)

print(out)
```

---

## 💡 Why it’s helpful
- Cleanly links related actions (e.g., support → payment)  
- Simplifies conversion attribution  
- Eliminates duplicate or invalid matches  
- Fully in-memory, pure-Python, and vectorized with NumPy  

---

## 📦 Requirements
```
pandas >= 1.5  
numpy >= 1.23
```

---

## 🧩 License
MIT © [Your Name]

# Strategy Documentation: [BOT018:RSI_MEAN_REVERSION]

## Data Description
**Data Sources:**  
Cybotrade Datasource API (Coinglass)

**Data Details:**  
- **Frequency:** [e.g., 1-hour candles]  
- **Variables:** [e.g., OHLCV, on-chain metrics]  
- **Timeframe:** [e.g., 2015–2024]  

**Preprocessing Steps:**  
1. Clean data  
2. Handle missing data  
3. Normalization   

---

## Strategy Logic
**Type:**  
[Specify the strategy type, e.g., Trend Following, Mean Reversion, Arbitrage.]  

**Detailed Steps:**  
1. [Describe the step-by-step execution of the strategy.]  
2. [Include all decision rules and thresholds.]  

**Parameters:**  
- Lookback Period: 30 days
- RSI < 30

---

## Live Implementation
**Infrastructure:**  
- **Platform:** Bybit  
- **Language/Framework:** Python

**Execution Logic:**  
- **Order Types(Limit/ Market):** Market  
- **Execution Algorithms:** [e.g., TWAP, VWAP]  

**Deployment Details:**  
Deploy Tool: Cybotrade Cloud

**Monitoring:**  
Monitor Tool: Bybit

---

## Backtest Performance
**Metrics:**
Initial Balance:   
Final Balance:  
Position Size:  
Trading Fee:  
Leverage:  
Sharpe Ratio:  
Annual Percentage Rate:  
Max Drawdown:  
Win Rate:  
**Equity Curve Graph**  

**Trade Signal Graph**  

**Sharpe Ratio Heatmap**  

**Calmar Ratio Graph**  


## Live Trading  
**Metrics:**
Initial Balance:   
Final Balance:  
Sharpe Ratio:  
Annual Percentage Rate: 
Max Drawdown:  
Win Rate:  
- **Period:** [e.g., Jan–Dec 2024]  
- **Metrics:** [e.g., Return, Sharpe Ratio, Drawdown, Win Rate]  


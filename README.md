# forex-questions

###  Question 1: Currency Momentum Metrics

In this task, I analyzed momentum patterns in currency exchange rates by detecting **streaks of consecutive daily increases**. Specifically, I calculated two metrics for each currency:

- `avg_cons_pos_days`: The average length of upward streaks (minimum 2 days)
- `avg_cons_perc_change`: The average percentage gain over those streaks

I used window functions to compare each day's rate with the previous day's, grouped consecutive positive changes into streaks, and then calculated these metrics per currency. Finally, I ranked the currencies on both and returned the top performers.

> This approach helps identify currencies with consistent upward trends and strong momentum over time.


### Question 2: Custom Behavioral Clustering

For this question, I designed two custom metrics that capture different dimensions of currency behavior:

- `volatility`: Measured as the standard deviation of daily percent changes, reflecting short-term price fluctuations
- `range_ratio`: The ratio of the max to min exchange rate over time, capturing overall price spread

Using these metrics, I clustered currencies into behavioral groups like:
- `volatile_high_range`
- `stable_low_range`
- `moderate`

> This helps differentiate currencies that are erratic versus stable, and those that fluctuate within narrow or wide long-term ranges.

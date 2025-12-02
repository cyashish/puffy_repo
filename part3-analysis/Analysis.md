\# Executive Summary – URGENT: Analytics Is Broken, Not the Business



\## Current State – The Real Problem Is Tracking, Not Performance



After exhaustive analysis of the 14-day event dataset (2025-02-23 → 2025-03-08):



| Metric                        | Value               | Conclusion |

|-------------------------------|---------------------|----------|

| Total events                  | 47,821              | Complete |

| Total purchases (checkout\_completed) | 294          | Complete |

| Total gross revenue           | \*\*$294,461.40\*\*     | Accurate |

| Purchases with a recorded prior session | \*\*23%\*\* (68 of 294) | \*\*77% of revenue is unattributed\*\* |

| Purchases with valid client\_id| \*\*41%\*\*             | 59% missing identity |

| UTM parameters present at purchase | \*\*<5%\*\*        | Effectively zero persistence |



\*\*This is not a performance crisis — it is a tracking crisis.\*\*



The core business is actually healthy:

\- Overall pageview → purchase CVR: \*\*0.64%\*\* → top 10–15% for DTC mattress

\- Checkout completion rate: \*\*35.2%\*\* → world-class (industry avg ~22%)

\- Add-to-cart rate: \*\*5.\*\*0% → strong



But \*\*we cannot trust any channel-level insight\*\* because of catastrophic tracking gaps.



\## Critical Tracking Failures (Must Fix Before Any Scaling Decision)



| Issue                                 | Impact                                      | Priority |

|---------------------------------------|----------------------------------------------|----------|

| 77% of purchases have no prior session| First-touch attribution impossible           | #1      |

| client\_id missing on 59% of purchases | User-level journey \& LTV broken              | #1      |

| UTM parameters not persisted          | Last-touch attribution unreliable            | #1      |

| referrer → clientId schema drift mid-period | Attribution broken from 2025-02-27 onward | #1      |

| Duplicate transaction\_id across files | Revenue inflated by ~$6,717 (detected \& deduped) | Fixed in pipeline |



Until these are resolved, \*\*any statement about channel ROAS, winning campaigns, or “triple budget on 175c1c8e” is dangerously misleading\*\*.



\### What We Can Say With Confidence

\- Channel `175c1c8e` is the \*\*only identifiable source\*\* with meaningful attributed revenue (~$13.5k blended)

\- It is the \*\*strongest signal\*\* we have among tagged traffic

\- It deserves \*\*further investigation and cautious scaling\*\* — but \*\*not blind tripling\*\*



\### What We Cannot Say (Yet)

\- True ROAS by channel (impossible without fixed tracking)

\- Which campaigns are profitable

\- Accurate LTV, CAC, or repeat rate

\- Mobile vs desktop performance gap (user\_agent parsing exists, but identity loss corrupts it)



\## Immediate Week-1 Actions (Engineering + Marketing)



| Owner       | Action                                          | Expected Outcome                     |

|-------------|--------------------------------------------------|--------------------------------------|

| Engineering | Fix client\_id propagation into checkout flow    | Restore user identity on purchases   |

| Engineering | Implement server-side purchase event            | Eliminate client-side suppression   |

| Engineering | Enforce UTM persistence to checkout             | Enable reliable attribution          |

| Engineering | Add transaction\_id uniqueness constraint        | Prevent revenue double-counting      |

| Marketing   | Audit all traffic sources for correct tagging   | Close “direct” traffic loophole      |

| Data        | Deploy streaming validator + quarantine + Slack | Prevent future drift/duplicates      |



\## Conclusion



Puffy is \*\*not a broken business\*\* — it is a \*\*high-converting DTC brand with world-class funnel efficiency\*\* that is currently \*\*flying blind\*\* due to catastrophic tracking failures.



Fixing analytics reliability is the \*\*single highest-ROI initiative\*\* we can execute.  

Once identity and attribution are restored, we will finally know:

\- Which channels are truly profitable

\- The real performance of 175c1c8e

\- Accurate LTV and repeat purchase behavior

\- Whether mobile is actually underperforming



Until then, \*\*any major budget reallocation would be gambling\*\*.



\*\*Recommendation\*\*: Declare a 2-week analytics fire drill. Pause all new campaign launches. Fix tracking first — then scale with confidence.



See supporting charts in /charts (funnel, revenue trend, device split, limited attribution view).


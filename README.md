## Dataset Overview

- **Original records:** 4,615
- **Final records after cleaning:** 3,865
- **Duplicate records removed:** 2
- **Records removed because both `total_laid_off` and `percentage_laid_off` were NULL:** 748
- **Unique companies:** 2,609
- **Total recorded layoffs:** 932,322
- **Date range:** March 11, 2020 – September 23, 2026
- **Note:** 2026 is an incomplete year in this dataset and should not be directly compared with full calendar years.

## Key Findings

### 1. Layoffs peaked in 2023

2023 recorded the highest annual layoff volume:

- **2020:** 80,998
- **2021:** 15,823
- **2022:** 164,319
- **2023:** **265,660**
- **2024:** 152,922
- **2025:** 122,606
- **2026:** 129,994 *(year-to-date)*

2023 accounted for approximately **28.5% of all recorded layoffs** in the cleaned dataset.

A notable pattern is the sharp increase from 2021 through 2023, followed by a decline in 2024 and 2025.

### 2. January 2023 was the highest-layoff month

The largest monthly layoff total occurred in **January 2023**, with approximately **89,709 recorded layoffs**.

This represents roughly **9.6% of all layoffs** in the dataset.

Other high-layoff months included:

- November 2022 – 53,594
- February 2023 – 40,002
- March 2023 – 37,963
- January 2024 – 34,137

This shows that layoffs were particularly concentrated around late 2022 and early 2023.

### 3. The United States accounted for the majority of recorded layoffs

The United States recorded approximately **661,113 layoffs**, representing about **70.9%** of the dataset total.

Other major countries included:

- India – 67,889
- Germany – 32,153
- United Kingdom – 23,944
- Netherlands – 22,175
- Sweden – 20,579

This should be interpreted as **absolute recorded layoff volume**, not as a layoff rate. The dataset does not contain total employment figures needed to compare the relative risk of layoffs between countries.

### 4. Amazon had the highest cumulative recorded layoffs

The companies with the highest cumulative recorded layoffs were:

| Company | Recorded Layoffs |
|---|---:|
| Amazon | 59,560 |
| Intel | 43,167 |
| Meta | 35,700 |
| Microsoft | 35,355 |
| Dell | 23,650 |
| Oracle | 22,294 |
| Cisco | 18,521 |
| Salesforce | 16,744 |
| Tesla | 14,500 |
| Google | 13,749 |

The top 10 companies together accounted for approximately **30% of all recorded layoffs**, showing that layoff volume was concentrated among a relatively small group of large companies.

### 5. Layoffs were spread across several industries

Major industries by recorded layoffs included:

- Other – 116,665
- Retail – 108,495
- Hardware – 105,532
- Consumer – 98,654
- Transportation – 70,273
- Finance – 66,496
- Food – 52,301
- Healthcare – 40,680

The results show that layoffs were not limited to a single industry. Several sectors contributed substantial layoff volumes.

Because the dataset uses separate industry categories such as Hardware, Consumer, and Infrastructure, it would be misleading to combine or label all of these as a single “technology” category without additional classification.

### 6. Large-scale layoffs occurred across different companies over time

The companies with the highest annual layoffs changed considerably:

- **2020:** Uber, Booking.com
- **2021:** Byju's, Katerra, Zillow
- **2022:** Meta, Amazon
- **2023:** Amazon, Google, Microsoft, Meta
- **2024:** Intel, Tesla, Cisco
- **2025:** Intel, Microsoft, Amazon
- **2026:** Oracle, Amazon, Dell

This suggests that the companies contributing the highest layoff volumes varied by year rather than being consistently dominated by one company.

### 7. Some records represent complete workforce reductions

The analysis identified **374 records where `percentage_laid_off = 1`**, meaning the recorded layoff represented 100% of the workforce for that event.

These records include companies such as Katerra, Redbox, Butler Hospitality, Lilium, Bitwise Industries, Zulily, Deliv, Eaze, Jump, and Convoy.

This should be interpreted at the **record/event level** unless the analysis is further refined to count distinct companies.

### 8. The largest single recorded layoff event involved 22,000 employees

The maximum value of `total_laid_off` was **22,000 employees**.

The maximum recorded `percentage_laid_off` was **100%**.

These extreme values demonstrate the presence of significant outliers in the dataset and highlight why both absolute layoffs and percentage layoffs are useful measures.

## Overall Takeaway

The analysis shows that layoffs were highly concentrated across specific periods, countries, companies, and industries.

The strongest overall pattern was the sharp increase in layoffs from **2022 to 2023**, with **2023 becoming the peak year** and **January 2023 recording the largest monthly total**.

The United States represented the majority of recorded layoffs, while a relatively small number of large companies accounted for a substantial share of total layoffs. However, the companies driving the highest layoff volumes changed considerably from year to year.

> **Data note:** The analysis is based on the uploaded dataset and its available records. The dataset ends on September 23, 2026, so 2026 figures are year-to-date and should not be treated as a complete annual total.
"""

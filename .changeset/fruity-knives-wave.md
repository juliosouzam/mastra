---
'@mastra/oracledb': patch
---

Fixed late workflow step results recreating deleted Oracle runs. Step-result updates now leave missing runs unchanged, while existing runs retain atomic updates.

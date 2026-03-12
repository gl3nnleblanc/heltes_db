---- MODULE MC ----
EXTENDS HeltesDB

\* Model values used as symbolic constants
CONSTANTS t1, t2, k1, v1, v2, c1, s1

MC_CoordOf == (t1 :> c1 @@ t2 :> c1)
MC_ShardOf == (k1 :> s1)
====

# Indicateurs actifs

P(t) = (a·F(t)+b·H(t)+c·L(t)+d·O(t))/(a+b+c+d)

δj(t) = (P(t)-P(t-1))/P(t-1)

M(t) = max(P(t-1)...P(t-N1))
X(t) = min(P(t-1)...P(t-N1))
M1(t) = moyenne sur N2 de M
X1(t) = moyenne sur N2 de X
T(t) = (M1(t)-X1(t))/e
Q(t) = M1(t)-T(t)
S(t) = M1(t)+T(t)
K1 = P-M1
K2 = P-X1
K3 = P-Q
K4 = P-S

p(t) = somme sur N2 de δj
Kf(t) = M1(t) - T(t)·p(t)

SUM_SLOPE(t) = somme sur Npente de δj
SLOPE_VRAI(t) = (P(t) - P(t-N2)) / P(t-N2)

Signaux actifs: A1, B1, C1, D1, E1, F1, G1, H1, Af, Bf, SPa, SPv, SPVa, SPVv.

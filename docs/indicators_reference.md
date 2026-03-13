# StockAlert — Référence indicateurs

Cette note reprend la page d'aide de l'application et sert de référence projet pour :
- les variables de scénario
- les indicateurs legacy
- les lignes flottantes
- les signaux
- les métriques Games

## Prix composite

P(t) = (a·F(t) + b·H(t) + c·L(t) + d·O(t)) / (a+b+c+d)

## Rendement journalier

δj(t) = (P(t) - P(t-1)) / P(t-1)

## Legacy

M(t)  = max(P(t-1), ..., P(t-N1))
X(t)  = min(P(t-1), ..., P(t-N1))
M1(t) = moyenne sur N2 jours de M(t)
X1(t) = moyenne sur N2 jours de X(t)
T(t)  = (M1(t) - X1(t)) / e
Q(t)  = M1(t) - T(t)
S(t)  = M1(t) + T(t)

K1(t) = P(t) - M1(t)
K2(t) = P(t) - X1(t)
K3(t) = P(t) - Q(t)
K4(t) = P(t) - S(t)

## K1f

E(t)       = M1(t) - X1(t)
ratio_p(t) = RATIO_P(t) / 100
C(t)       = (VC - ratio_p(t)) · FL · E(t)
K1f(t)     = K1(t) + C(t)

## K2f

slope1(t)     = moyenne sur N1 jours de δj(t)
slope2(t)     = moyenne sur N2 jours de δj(t)
slope_deg(t)  = slope1(t) - slope2(t)
h             = max(1, floor(N5/2))
Mf1(t)        = moyenne sur h jours des max de P sur des fenêtres glissantes de taille N5
Xf1(t)        = moyenne sur h jours des min de P sur des fenêtres glissantes de taille N5
Ef(t)         = Mf1(t) - Xf1(t)
K2f(t)        = Mf1(t) - slope_deg(t) · cr · Ef(t) / e

## Kf2bis

p(t)      = somme sur N2 jours de δj(t)
Kf2bis(t) = Mf1(t) - Ef(t) · p(t)

## Kf3

r(t)            = (P(t) - P(t-1)) / P(t-1)
amp(t)          = moyenne sur NampL3 jours de |r(t)|
periode(t)      = periodeL3 · baseL3 / amp(t)
slope_degL3(t)  = moyenne de r(t) sur periode(t)
Mf1L3(t)        = moyenne locale des max de P sur N5f3
Xf1L3(t)        = moyenne locale des min de P sur N5f3
EfL3(t)         = Mf1L3(t) - Xf1L3(t)
Kf3(t)          = Mf1L3(t) - slope_degL3(t) · crf3 · EfL3(t) / e

## SUM_SLOPE / SPa / SPv

SUM_SLOPE(t) = somme sur Npente jours de δj(t)

SPa:
- SUM_SLOPE(t-1) < Seuil_de_pente
- SUM_SLOPE(t)   > Seuil_de_pente

SPv:
- SUM_SLOPE(t-1) > Seuil_de_pente
- SUM_SLOPE(t)   < Seuil_de_pente

## Signaux disponibles

A1, B1, A1f, B1f, C1, D1, E1, F1, G1, H1, AF, BF, A2bis, B2bis, AF3, BF3, I1, J1, SPa, SPv

## Games

Condition de tradabilité :

BMD >= tradability_threshold
SUM_SLOPE >= slope_threshold
RATIO_IN_POSITION >= presence_threshold_pct

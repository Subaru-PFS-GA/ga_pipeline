OBSRUN="2025-03"
RERUN="run21_June2025"
OBSLOGS="../Subaru-PFS/spt_ssp_observation/runs/$OBSRUN/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

gapipe-catalog --rerundir $RERUN --visit $VISITS --obs-log $OBSLOGS --objid 0x200000000-0x2FFFFFFFF
gapipe-catalog --rerundir $RERUN --visit $VISITS --obs-log $OBSLOGS --objid 0x600000000-0x6FFFFFFFF

###

OBSRUN="2025-05"
RERUN="run22_July2025"
OBSLOGS="../Subaru-PFS/spt_ssp_observation/runs/$OBSRUN/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

gapipe-catalog --rerundir $RERUN --visit $VISITS --obs-log $OBSLOGS --objid 0x1000000000-0x1FFFFFFFFF

###

OBSRUN="2025-06"
RERUN="run23_August2025"
OBSLOGS="../Subaru-PFS/spt_ssp_observation/runs/$OBSRUN/obslog/*.csv"
VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

gapipe-catalog --rerundir $RERUN --visit $VISITS --obs-log $OBSLOGS --objid 0x7000000000-0x7FFFFFFFFF
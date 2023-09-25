# Multi-resolution database making use of scaled

## Example using prefetch

```
% ./prefetch.py SRR606249-k31.s1000.zip podar-ref.zip    
layer100000 searched 64
best match layer100000: 81 limit={8}
limiting to: {8}
layer10000 searched 1
best match layer10000: 901 limit={8}
limiting to: {8}
layer1000 searched 1
best match layer1000: 9323 limit={8}
overlap=9323, name='NC_007951.1 Burkholderia xenovorans LB400 chromosome 1, complete sequence'
```

### Example using gather

```
% ./gather.py SRR606249-k31.s1000.zip podar-ref.zip | grep MATCH
MATCH: 9323 NC_007951.1 Burkholderia xenovorans LB400 chromosome 1, complete sequence
MATCH: 7288 NC_003272.1 Nostoc sp. PCC 7120 DNA, complete genome
MATCH: 7039 BX119912.1 Rhodopirellula baltica SH 1 complete genome
MATCH: 6604 NC_009972.1 Herpetosiphon aurantiacus DSM 785, complete genome
MATCH: 5638 CP000850.1 Salinispora arenicola CNS-205, complete genome
MATCH: 6259 AE015928.1 Bacteroides thetaiotaomicron VPI-5482, complete genome
MATCH: 5148 NZ_JGWU01000001.1 Bordetella bronchiseptica D989 ctg7180000008197, whole genome shotgun sequence
MATCH: 5049 CP000909.1 Chloroflexus aurantiacus J-10-fl, complete genome
MATCH: 4954 CP000139.1 Bacteroides vulgatus ATCC 8482, complete genome
MATCH: 5422 AE010299.1 Methanosarcina acetivorans str. C2A, complete genome
MATCH: 4807 CP000667.1 Salinispora tropica CNB-440, complete genome
MATCH: 4754 CP001013.1 Leptothrix cholodnii SP-6, complete genome
MATCH: 5188 NC_011663.1 Shewanella baltica OS223, complete genome
MATCH: 4168 CP001472.1 Acidobacterium capsulatum ATCC 51196, complete genome
MATCH: 4628 AP009153.1 Gemmatimonas aurantiaca T-27 DNA, complete genome
MATCH: 3655 CP000568.1 Clostridium thermocellum ATCC 27405, complete genome
MATCH: 3858 NZ_CH959317.1 Sulfitobacter sp. NAS-14.1 scf_1099451320472, whole genome shotgun sequence
...
```

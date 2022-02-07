
Universe_SP500 = ['MMM', 'ABT', 'ABBV', 'ACN', 'ATVI', 'ZION', 'ZTS']
Universe_SP100 = ['AMZN', 'ASML', 'ATVI', 'AVGO', 'BIDU', 'BIIB', 'BKNG', 'BMRN', 'CA', 'CDNS']
Famous = ['AMZN', 'AAPL', 'F','TSLA','FB']
Spiders = [
    'XLB', # Materials
    'XLE', # Energy
    'XLF', # Financials
    'XLI', # Industrials
    'XLK', # Technology
    'XLP', # Staples
    'XLY', # Discretionaries
    'XLU', # Utilities
    'XLV', # Healthcare
]   

SP500_memb_2011_2021_RIC = ['SNI.OQ^C18', 'ZBH.N', 'SRE.N', 'KTB.N', 'TWTR.N', 'CSCO.OQ', 'ETSY.OQ', 'JNJ.N', 'DTM.N', 'MO.N', 
                        'XYL.N', 'RAI.N^G17', 'CBE.N^L12', 'HON.OQ', 'STZ.N', 'SPLS.OQ^I17', 'LM.N^H20', 'CE.N', 'PLD.N', 'MLM.N', 'TSN.N', 
                        'SUNE.N^D16', 'CMCSA.OQ', 'DXC.N', 'IRM.N', 'TWC.N^E16', 'LW.N', 'HSY.N', 'JWN.N', 'CPGX.N^G16', 'IPGP.OQ', 'ES.N', 
                        'FTI.N', 'GPC.N', 'SNPS.OQ', 'HSIC.OQ', 'MMI.N^E12', 'VAL.N^H20', 'NDAQ.OQ', 'EMC.N^I16', 'R.N', 'MXIM.OQ^H21', 'WDC.OQ', 
                        'XRX.OQ', 'PCP.N^B16', 'XEL.OQ', 'MSI.N', 'NGVT.N', 'KSS.N', 'FRC.N', 'URI.N', 'BBWI.N', 'BHF.OQ', 'APC.N^H19', 'REZI.N', 
                        'HES.N', 'APTV.N', 'APH.N', 'TSLA.OQ', 'ROST.OQ', 'MCD.N', 'CERN.OQ', 'YUM.N', 'ETFC.OQ^J20', 'PH.N', 'FAST.OQ', 'SEDG.OQ', 
                        'HBI.N', 'CZR.OQ', 'DISH.OQ', 'WAB.N', 'IT.N', 'AJG.N', 'HAS.OQ', 'CVH.N^E13', 'CARS.N', 'AME.N', 'LVLT.N^K17', 'LO.N^F15', 
                        'FCX.N', 'NSC.N', 'RCL.N', 'FCPT.N', 'DELL.OQ^J13', 'ATI.N', 'TGT.N', 'WYNN.OQ', 'INCY.OQ', 'PLD.N^F11', 'EVRG.N', 'LEG.N', 
                        'CAH.N', 'CPRT.OQ', 'STT.N', 'JKHY.OQ', 'SRCL.OQ', 'HNZ.N^F13', 'JNPR.N', 'CLX.N', 'MWV.N^G15', 'WHR.N', 'ARG.N^E16', 'DHR.N', 
                        'LOW.N', 'TE.N^G16', 'DE.N', 'CNC.N', 'TPR.N', 'AWK.N', 'JEF.N', 'LYB.N', 'KSU.N^L21', 'PENN.OQ', 'ATGE.N', 'TDG.N', 'HOT.N^I16', 
                        'FLS.N', 'ETN.N', 'CVET.OQ', 'ONL.N', 'SCHW.N', 'PWR.N', 'TRV.N', 'NOVL.OQ^D11', 'GPN.N', 'PLL.N^H15', 'TMO.N', 'HOLX.OQ', 
                        'EOG.N', 'NVR.N', 'F.N', 'BTU.N^D16', 'WPX.N^A21', 'M.N', 'TYL.N', 'KO.N', 'MFE.N^C11', 'MMC.N', 'MA.N', 'MHS.N^D12', 'PRU.N', 
                        'BRCM.OQ^B16', 'BIO.N', 'TEG.N^F15', 'EW.N', 'GS.N', 'MAS.N', 'ALTR.OQ^L15', 'EVHC.N^L16', 'MAR.OQ', 'VTR.N', 'BK.N', 'PCG.N', 
                        'VRSK.OQ', 'UPS.N', 'IR.N', 'FFIV.OQ', 'XRAY.OQ', 'PCL.N^B16', 'BEAM.N^E14', 'GE.N', 'NFLX.OQ', 'ISRG.OQ', 'RHI.N', 'CL.N', 'UHS.N', 
                        'ITW.N', 'SHW.N', 'CARR.N', 'LDOS.N', 'NTAP.OQ', 'CVX.N', 'TFCF.OQ^C19', 'EXC.OQ', 'AGN.N^E20', 'XEC.N^J21', 'OGN.N', 'LLY.N', 'WRB.N', 
                        'RTN.N^D20', 'BRO.N', 'CHTR.OQ', 'MTCH.OQ', 'O.N', 'MTB.N', 'GOOG.OQ', 'INFO.N', 'WU.N', 'GLW.N', 'TXT.N', 'AAP.N', 'FHN.N', 'C.N', 
                        'XL.N^I18', 'KLAC.OQ', 'NWL.OQ', 'NXPI.OQ', 'ABC.N', 'T.N', 'MKC.N', 'SLG.N', 'WY.N', 'KR.N', 'JNS.N^E17', 'STE.N', 'MKTX.OQ', 'TMUS.OQ', 
                        'ANR.N^G15', 'PNW.N', 'AMT.N', 'TDC.N', 'ALLE.N', 'D.N', 'YUMC.N', 'SE.N^B17', 'COP.N', 'HRL.N', 'ABMD.OQ', 'JCI.N^I16', 'ATVI.OQ', 
                        'DOW.N^I17', 'DD.N', 'HOG.N', 'CELG.OQ^K19', 'WEC.N', 'BLK.N', 'JCI.N', 'AMD.OQ', 'UAL.OQ', 'NWS.OQ', 'CLF.N', 'PEG.N', 'WCG.N^A20', 
                        'TGNA.N', 'CVS.N', 'CB.N', 'GR.N^G12', 'CCEP.OQ', 'BSX.N', 'PAYX.OQ', 'COL.N^K18', 'NFX.N^B19', 'FLIR.OQ^E21', 'IP.N', 'NLOK.OQ', 
                        'AZO.N', 'TDY.N', 'CCL.N', 'DVN.N', 'WBA.OQ', 'GME.N', 'PBCT.OQ', 'VTRS.OQ', 'CNDT.OQ', 'PG.N', 'ILMN.OQ', 'NKTR.OQ', 'NBL.OQ^J20', 
                        'FOSL.OQ', 'FL.N', 'CTVA.N', 'DRI.N', 'CXO.N^A21', 'AXP.N', 'ZTS.N', 'ABBV.N', 'MEE.N^F11', 'NBR.N', 'MTD.N', 'NRG.N', 'BMC.OQ^I13', 
                        'BBY.N', 'ADNT.N', 'TER.OQ', 'VLO.N', 'CTAS.OQ', 'AA.N', 'KDP.OQ', 'AMP.N', 'ADM.N', 'LEN.N', 'DG.N', 'GWW.N', 'DGX.N', 'INTC.OQ', 
                        'SBNY.OQ', 'SLVM.N', 'HLT.N', 'ANF.N', 'ECL.N', 'MU.OQ', 'ITT.N', 'JBL.N', 'VREX.OQ', 'HWM.N', 'MSCI.N', 'RMD.N', 'DISCK.OQ', 'IQV.N', 
                        'EA.OQ', 'NUE.N', 'PAYC.N', 'CFG.N', 'NTRS.OQ', 'MWW.N^K16', 'HUM.N', 'FRX.N^G14', 'VMC.N', 'TRMB.OQ', 'UDR.N', 'NE.N^H20', 'CRM.N', 
                        'BFb.N', 'DHI.N', 'GM.N', 'LMT.N', 'IILGV.OQ^E16', 'FTNT.OQ', 'CME.OQ', 'AGN.N^C15', 'ANET.N', 'PPG.N', 'ALB.N', 'NSM.N^I11', 'ENPH.OQ', 
                        'WRK.N', 'BEN.N', 'MCHP.OQ', 'CTSH.OQ', 'AN.N', 'AAL.OQ', 'GMCR.OQ^C16', 'CI.N', 'AABA.OQ^J19', 'SCG.N^A19', 'CSRA.N^D18', 'SYY.N', 
                        'AMAT.OQ', 'MCK.N', 'AIZ.N', 'PTC.OQ', 'BIVV.OQ^C18', 'ROP.N', 'CTLT.N', 'BMY.N', 'KMB.N', 'PPL.N', 'MCO.N', 'FDO.N^G15', 'APD.N', 
                        'RHT.N^G19', 'GPS.N', 'PDCO.OQ', 'LLL.N^G19', 'SEE.N', 'AET.N^K18', 'XOM.N', 'K.N', 'WAT.N', 'MI.N^G11', 'CAM.N^D16', 'CF.N', 'DAL.N', 
                        'COV.N^A15', 'DELL.N', 'HCBK.OQ^K15', 'UAA.N', 'S.N^G13', 'ADSK.OQ', 'PRSP.N^E21', 'STI.N^L19', 'DNR.N^G20', 'AYE.N^B11', 'MPWR.OQ', 
                        'FITB.OQ', 'MOLX.OQ^L13', 'PRGO.N', 'FTR.OQ^D20', 'KMI.N', 'TEL.N', 'GTX.N^I20', 'WFC.N', 'ORLY.OQ', 'DNB.N^B19', 'BCR.N^L17', 'DVA.N', 
                        'ACN.N', 'EPAM.N', 'VAR.N^D21', 'FRT.N', 'RTX.N', 'TT.N', 'WFM.OQ^H17', 'CHX.OQ', 'MNK.N^J20', 'DTE.N', 'BKR.OQ', 'CNP.N', 'SPG.N', 
                        'PHM.N', 'NOC.N', 'AVY.N', 'BBBY.OQ', 'HD.N', 'RSH.N^B15', 'THC.N', 'AEE.N', 'MSFT.OQ', 'HFC.N', 'FSLR.OQ', 'GD.N', 'HRB.N', 'WST.N', 
                        'DD.N^I17', 'MAC.N', 'ASIX.N', 'ODFL.OQ', 'AOS.N', 'IGT.N^D15', 'DLTR.OQ', 'SBAC.OQ', 'DTV.OQ^G15', 'Q.N^D11', 'HP.N', 'FBHS.N', 'PXD.N', 
                        'RJF.N', 'HPQ.N', 'LVS.N', 'KEYS.N', 'EFX.N', 'MAA.N', 'BIG.N', 'DRE.N', 'SIG.N', 'ADP.OQ', 'IEX.N', 'TJX.N', 'CRL.N', 'BR.N', 'SYF.N', 
                        'CBOE.Z', 'MDT.N', 'PCAR.OQ', 'PYPL.OQ', 'FISV.OQ', 'AKS.N^C20', 'ANTM.N', 'FB.OQ', 'FE.N', 'SJM.N', 'EVHC.N^J18', 'SVU.N^J18', 'AVB.N', 
                        'ANSS.OQ', 'WMB.N', 'TFC.N', 'ABT.N', 'EIX.N', 'PM.N', 'HST.OQ', 'VSM.N^J19', 'NOW.N', 'QCP.N^G18', 'JBGS.N', 'HBAN.OQ', 'FLR.N', 
                        'EQR.N', 'MDP.N^L21', 'GAS.N^L11', 'QLGC.OQ^H16', 'AVGO.OQ', 'OKE.N', 'HIG.N', 'DO.N^D20', 'CPWR.OQ^L14', 'CFN.N^C15', 'VRTX.OQ', 
                        'BDX.N', 'LKQ.OQ', 'TWX.N^F18', 'KRFT.OQ^G15', 'ADBE.OQ', 'CAG.N', 'LNC.N', 'AON.N', 'AES.N', 'CHD.N', 'PGR.N', 'ENDP.OQ', 'SWK.N', 
                        'MRO.N', 'MAT.OQ', 'CMS.N', 'GIS.N', 'NOV.N', 'QEP.N^C21', 'TECH.OQ', 'IDXX.OQ', 'XLNX.OQ', 'KD.N', 'NVDA.OQ', 'SUN.N^J12', 'GNW.N', 
                        'POOL.OQ', 'LRCX.OQ', 'DPZ.N', 'RSG.N', 'VNO.N', 'BXP.N', 'FLT.N', 'BLL.N', 'PKI.N', 'V.N', 'HCA.N', 'NVT.N', 'OI.N', 'ROK.N', 
                        'POM.N^C16', 'RIG.N', 'MNST.OQ', 'EMR.N', 'PBI.N', 'HAR.N^C17', 'VIAC.OQ', 'MON.N^F18', 'ESS.N', 'JPM.N', 'VZ.N', 'CVC.N^F16', 'COO.N', 
                        'CPRI.N', 'PFE.N', 'MRNA.OQ', 'AYI.N', 'CHK.N^F20', 'RDC.N^D19', 'AIG.N', 'FTI.N^A17', 'CINF.OQ', 'VIAV.OQ', 'USB.N', 'OMC.N', 'MGM.N', 
                        'NVLS.OQ^F12', 'EBAY.OQ', 'WELL.N', 'APA.OQ', 'BA.N', 'DOW.N', 'GAS.N^G16', 'AIV.N', 'KIM.N', 'IPG.N', 'GT.OQ', 'GENZ.OQ^D11', 'LUMN.N', 
                        'AKAM.OQ', 'ADI.OQ', 'QRVO.OQ', 'KMX.N', 'HSP.N^I15', 'CEG.N^C12', 'NCLH.N', 'SO.N', 'CB.N^A16', 'ETR.N', 'BKNG.OQ', '8', 'EQT.N', 
                        'NI.N', 'MMM.N', 'SNDK.OQ^E16', 'PEP.OQ', 'MJN.N^F17', 'DF.N^K19', 'APOL.OQ^B17', 'DISCA.OQ', 'OXY.N', 'ANDV.N^J18', 'LYV.N', 
                        'SHLD.OQ^J18', 'SYK.N', 'MHK.N', 'VIAB.OQ^L19', 'REGN.OQ', 'PVH.N', 'X.N', 'CA.OQ^K18', 'URBN.OQ', 'NAVI.OQ', 'CMG.N', 'HPE.N', 
                        'STX.OQ', 'GHC.N', 'REG.OQ', 'BAX.N', 'CDNS.OQ', 'SIAL.OQ^K15', 'VNT.N', 'SLB.N', 'ULTA.OQ', 'MS.N', 'DLPH.N^J20', 'FMC.N', 'HSH.N^H14', 
                        'ESRX.OQ^L18', 'PNC.N', 'VRSN.OQ', 'RRD.N', 'GNRC.N', 'TROW.OQ', 'LHX.N', 'LUV.N', 'TNL.N', 'ZION.OQ', 'AEP.OQ', 'FANG.OQ', 'EQIX.OQ', 
                        'ALL.N', 'BRKb.N', 'TAP.N', 'JOY.N^D17', 'STJ.N^A17', 'A.N', 'CTXS.OQ', 'IVZ.N', 'ADT.N^E16', 'CBRE.N', 'WMT.N', 'HAL.N', 'RRC.N', 
                        'LLTC.OQ^C17', 'AMG.N', 'GILD.OQ', 'RE.N', 'BXLT.N^F16', 'TTWO.OQ', 'NYX.N^K13', 'WTW.OQ', 'SNA.N', 'PKG.N', 'CPB.N', 'FDS.N', 
                        'CEPH.OQ^J11', 'ADS.N', 'ARE.N', 'UNH.N', 'ED.N', 'BWA.N', 'MRK.N', 'FIS.N', 'LIN.N', 'AFL.N', 'PNR.N', 'SWY.N^A15', 'FTV.N', 'CNX.N', 
                        'DIS.N', 'CMA.N', 'MUR.N', 'AIRC.N', 'AMCR.N', 'FHI.N', 'DFS.N', 'LIFE.OQ^B14', 'GL.N', 'COST.OQ', 'SWKS.OQ', 'MPC.N', 'LSI.OQ^E14', 
                        'VFC.N', 'QCOM.OQ', 'MDLZ.OQ', 'IFF.N', 'TIF.N^A21', 'CMI.N', 'TSCO.OQ', 'TLAB.OQ^L13', 'TSS.N^I19', 'AMZN.OQ', 'PFG.OQ', 'AVP.N^A20', 
                        'DOV.N', 'WM.N', 'SIVB.OQ', 'ORCL.N', 'TFX.N', 'NEE.N', 'ICE.N', 'SPGI.N', 'JBHT.OQ', 'BAC.N', 'EXPE.OQ', 'FOX.OQ', 'VSCO.N', 'GRMN.N', 
                        'ROL.N', 'ZBRA.OQ', 'EXPD.OQ', 'ALK.N', 'CDW.OQ', 'DUK.N', 'CMCSK.OQ^L15', 'TIE.N^A13', 'HII.N', 'MET.N', 'AAPL.OQ', 'DXCM.OQ', 
                        'GOOGL.OQ', 'TXN.OQ', 'EMN.N', 'LNT.OQ', 'RL.N', 'MOS.N', 'CAT.N', 'WIN.OQ^C19', 'ALGN.OQ', 'PEAK.N', 'PGN.N^G12', 'CDAY.N', 'KEY.N', 
                        'NWSA.OQ', 'LXK.N^K16', 'CSX.OQ', 'BIIB.OQ', 'ALXN.OQ^G21', 'PETM.OQ^C15', 'UA.N', 'SBUX.OQ', 'IBM.N', 'NLSN.N', 'RF.N', 'EL.N', 
                        'DLR.N', 'PSX.N', 'COTY.N', 'UNP.N', 'TRIP.OQ', 'COF.N', 'TFCFA.OQ^C19', 'NEM.N', 'CHRW.OQ', 'SWN.N', 'ATO.N', 'PSA.N', 'KHC.OQ', 'LH.N', 'FDX.N']

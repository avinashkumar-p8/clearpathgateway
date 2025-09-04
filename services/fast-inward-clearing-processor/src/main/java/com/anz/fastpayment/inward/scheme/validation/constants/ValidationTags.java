package com.anz.fastpayment.inward.scheme.validation.constants;

/**
 * Constants for XML tag names and their exact JSON paths for direct field extraction.
 * These paths are used to directly fetch values from the Kafka message payload
 * without creating a full HashMap.
 */
public final class ValidationTags {
    private ValidationTags() {}
    
    // ===== TAG NAMES FOR SWITCH STATEMENT =====
    public static final String TAG_CURRENCY = "currency";
    public static final String TAG_COUNTRY = "country";
    public static final String TAG_FROM_MMBID = "from_mmbid";
    public static final String TAG_TO_MMBID = "to_mmbid";
    public static final String TAG_BIZ_MSG_IDR = "biz_msg_idr";
    public static final String TAG_INSTR_ID = "instr_id";
    public static final String TAG_CPY_DPICT = "cpy_dpict";
    // public static final String TAG_CRE_DT_TM = "cre_dt_tm";
    public static final String TAG_INTR_BK_STTLM_DT = "intr_bk_sttlm_dt";
    // public static final String TAG_NB_OF_TXS = "nb_of_txs";
    // public static final String TAG_TTL_INTR_BK_STTLM_AMT = "ttl_intr_bk_sttlm_amt";
    public static final String TAG_INTR_BK_STTLM_AMT = "intr_bk_sttlm_amt";
    // public static final String TAG_STTLM_MTD = "sttlm_mtd";
    // public static final String TAG_CD = "cd";
    // public static final String TAG_PMT_TP_INF_SVC_LVL_CD = "pmt_tp_inf_svc_lvl_cd";
    public static final String TAG_CDTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "cdtr_agt_fin_instn_id_clr_sys_mmb_id_mmb_id";
    public static final String TAG_INSTG_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "instg_agt_fin_instn_id_clr_sys_mmb_id_mmb_id";
    public static final String TAG_DBTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "dbtr_agt_fin_instn_id_clr_sys_mmb_id_mmb_id";
    public static final String TAG_INSTD_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "instd_agt_fin_instn_id_clr_sys_mmb_id_mmb_id";
    // public static final String TAG_CHRG_BR = "chrg_br";
    // public static final String TAG_ORGNL_MSG_NM_ID = "orgnl_msg_nm_id";
    // public static final String TAG_RVSL_ID = "rvsl_id";
    // public static final String TAG_RVSD_INTR_BK_STTLM_AMT = "rvsd_intr_bk_sttlm_amt";
    // public static final String TAG_RVSL_RSN_INF_RSN_PRTY = "rvsl_rsn_inf_rsn_prty";
    
    // ===== JSON PATHS FOR DIRECT FIELD EXTRACTION =====
    // ===== CURRENCY & COUNTRY =====
    public static final String CURRENCY = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmCCY";
    public static final String COUNTRY = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAcct.Country";
    
    // ===== MMBID FIELDS =====
    // public static final String FROM_MMBID = "Fr.FIId.FinInstnId.ClrSysMmbId.MmbId";
    // public static final String TO_MMBID = "To.FIId.FinInstnId.ClrSysMmbId.MmbId";
    
    // ===== MESSAGE IDENTIFIERS =====
    // public static final String BIZ_MSG_IDR = "Header.BizMsgIdr";
    public static final String INSTR_ID = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.PmtId.InstrId";
    
    // ===== COPY & DUPLICATE INDICATORS =====
    // public static final String CPY_DPICT = "Header.CpyDpict";
    
    // ===== DATE & TIME FIELDS =====
    // public static final String CRE_DT_TM = "instruction.MsgCtxt.EventTS";
    public static final String INTR_BK_STTLM_DT = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmDt";
    
    // ===== TRANSACTION COUNTS =====
    // public static final String NB_OF_TXS = "instruction.MsgCtxt.BaseAmt";
    
    // ===== AMOUNT FIELDS =====
    // public static final String TTL_INTR_BK_STTLM_AMT = "instruction.MsgCtxt.BaseAmt";
    public static final String INTR_BK_STTLM_AMT = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmAmt";
    
    // ===== SETTLEMENT METHOD =====
    // public static final String STTLM_MTD = "instruction.MsgCtxt.InstdClrgPref";
    
    // ===== CODE FIELDS =====
    // public static final String CD = "instruction.MsgCtxt.OrigMsgTyp";
    
    // ===== PAYMENT TYPE INFORMATION =====
    // public static final String PMT_TP_INF_SVC_LVL_CD = "instruction.MsgCtxt.InstdMoPCat";
    
    // ===== FINANCIAL INSTITUTION IDENTIFIERS =====
    public static final String CDTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.CdtrAgt.BIC";
    public static final String INSTG_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstgAgt.BIC";
    public static final String DBTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAgt.BIC";
    public static final String INSTD_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID = "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstdAgt.BIC";
    
    // ===== CHARGE BEARER =====
    // public static final String CHRG_BR = "instruction.MsgCtxt.ProcCtryCd";
    
    // ===== ORIGINAL MESSAGE NAME ID =====
    // public static final String ORGNL_MSG_NM_ID = "instruction.MsgDef.Schema";
    
    // ===== REVERSAL IDENTIFIER =====
    // public static final String RVSL_ID = "instruction.MsgCtxt.MsgId";
    
    // ===== REVERSED INTERBANK SETTLEMENT AMOUNT =====
    // public static final String RVSD_INTR_BK_STTLM_AMT = "instruction.MsgCtxt.BaseAmt";
    
    // ===== REVERSAL REASON INFORMATION =====
    // public static final String RVSL_RSN_INF_RSN_PRTY = "instruction.MsgCtxt.Direction";
}

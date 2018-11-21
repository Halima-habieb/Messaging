create or replace PACKAGE pkg_msg_util
AS
  msg_crlf VARCHAR2(2) := CHR(10);
  FUNCTION is_string_alpha(
      p_string VARCHAR2)
    RETURN BOOLEAN;
  FUNCTION is_string_number(
      p_string VARCHAR2)
    RETURN BOOLEAN;
  FUNCTION validate_string_length(
      p_string VARCHAR2,
      p_length NUMBER)
    RETURN BOOLEAN;
  FUNCTION split_string_line(
      p_string  VARCHAR2,
      p_line_no NUMBER)
    RETURN VARCHAR2;
  FUNCTION split_string_between(
      p_string VARCHAR2,
      p_from   VARCHAR2,
      p_to     VARCHAR2)
    RETURN VARCHAR2;
  FUNCTION is_valid_airport(
      p_airport VARCHAR2)
    RETURN BOOLEAN;
  PROCEDURE process_log(
      p_msgidr NUMBER,
      p_msglog VARCHAR2);
  FUNCTION get_customs_address(
      p_arpcod VARCHAR2 DEFAULT NULL,
      p_cuscod VARCHAR2 DEFAULT NULL)
    RETURN VARCHAR2;
  FUNCTION gen_envelop_header(
      p_awb_prefix VARCHAR2 DEFAULT NULL,
      p_awb_no     VARCHAR2 DEFAULT NULL,
      p_flt_id     NUMBER DEFAULT 0,
      p_flt_org    VARCHAR2 DEFAULT NULL,
      p_cus_cod    VARCHAR2 DEFAULT NULL)
    RETURN VARCHAR2;
END pkg_msg_util;

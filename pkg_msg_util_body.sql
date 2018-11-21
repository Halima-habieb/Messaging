create or replace package body pkg_msg_util as

  function is_string_alpha (
    p_string varchar2) return boolean
  as
    l_returnvalue boolean;
  begin
    l_returnvalue := regexp_instr(p_string, '[^a-z|A-Z]') = 0;
    return l_returnvalue;
  end is_string_alpha;

  function is_string_number(
    p_string varchar2) return boolean
  as
    l_returnvalue boolean;
    l_number      number;
  begin
    begin
      l_number := to_number(p_string);
      l_returnvalue := true;
    exception when others then
      dbms_output.put_line(SQLERRM);
      l_returnvalue := false;
    end;
    return l_returnvalue;
  end is_string_number;

  function validate_string_length(
    p_string varchar2,
    p_length number) return boolean as
    l_length number default 0;
  begin
    l_length := length(p_string);
    if l_length = p_length then
      return true;
    else
      return false;
    end if;
  end validate_string_length;

  function split_string_line(
    p_string varchar2,
    p_line_no number) return varchar2 as
    l_string varchar2(2000);
  begin

    if p_line_no = 1 then
      select substr(p_string, 1, instr(p_string, msg_crlf, 1, p_line_no)) into l_string from dual;
    else
      select substr(p_string, instr(p_string, msg_crlf, 1, p_line_no-1)+1,
          instr(p_string, msg_crlf, 1, p_line_no) - instr(p_string, msg_crlf, 1, p_line_no-1))
      into l_string from dual;
    end if;


    if instr(l_string, msg_crlf) > 0 then
      l_string := substr(l_string,1,length(l_string)-2);
    end if;

    return l_string;
  exception when others then
    return null;
  end split_string_line;


  function split_string_between(
    p_string varchar2,
    p_from varchar2,
    p_to varchar2) return varchar2 as
    l_string varchar2(2000);
  begin
    if p_to is not null then
      select substr(p_string, instr(p_string, p_from),
        instr(p_string, p_to)-instr(p_string, p_from)) into l_string from dual;
    else
      select substr(p_string, instr(p_string, p_from)) into l_string from dual;
    end if;
    return l_string;
  exception when others then
    return null;
  end split_string_between;

  function is_valid_airport(
    p_airport varchar2) return boolean
  as
  begin
    return true;
  end is_valid_airport;

  procedure process_log(
    p_msgidr number,
    p_msglog varchar2) as
    pragma autonomous_transaction;
  begin
    INSERT INTO MSG_LOG ( LOGSEQ, MSGIDR, MSGLOG, LOGTIM )
    VALUES (
      trx_seq.nextval, p_msgidr, p_msglog, sysdate
    );
    commit;
  end;

  function get_customs_address(p_arpcod varchar2 default null, p_cuscod varchar2 default null) return varchar2
  as
    l_cusadd varchar2(20);
  begin
    -- select dstadd into l_cusadd from stations_m where stn_code = p_arpcod;  TODO chek the table column dstadd
    return l_cusadd;
  exception when no_data_found then
    return 'IADFACI';
  end;

  function gen_envelop_header(p_awb_prefix varchar2 default null,
      p_awb_no     varchar2 default null,
      p_flt_id     number   default 0,
      p_flt_org    varchar2 default null,
      p_cus_cod    VARCHAR2 DEFAULT NULL) return varchar2
  as
    p_org varchar2(3);
    p_dst varchar2(3);
    p_ubr number;
    p_env varchar2(100);
  begin

    if p_cus_cod is not null then
      p_env := 'QK' || ' ' || get_customs_address(p_cuscod => p_cus_cod) || msg_crlf;
    elsif p_awb_prefix is not null and p_awb_no is not null then
      select ubr into p_ubr from bkgmst where awbpre = p_awb_prefix and awbnum = p_awb_no and rownum < 2;

      for f in (select * from bkgflt where ubr = p_ubr and fltseq = p_flt_id)
      loop
        p_env := 'QK' || ' ' || get_customs_address(p_arpcod => f.fltdst) || msg_crlf;
      end loop;
    elsif p_flt_id > 0 then
      for f in (select * from fltleg where fltseq = p_flt_id and legorg = p_flt_org)
      loop
        p_env := 'QK' || ' ' || get_customs_address(p_arpcod => f.legdst) || msg_crlf;
      end loop;
    end if;

    return p_env;

  exception when no_data_found then
    return null;
  end gen_envelop_header;

end pkg_msg_util;

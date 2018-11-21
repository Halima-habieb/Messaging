create or replace PACKAGE BODY pkg_process_fhl
AS

-- -----------------------------------------------------------------------------
-- Name         : PKG_PROCESS_FHL
-- Description  : Process FHL Messages
-- -----------------------------------------------------------------------------


  v_message_without_envelope VARCHAR2(2000);
  l_log         varchar2(4000);
  r_awb rec_awb;
-- Functin get message
  function get_message(p_msg_id number) return varchar2
  as
  begin
    select msgdta into v_message_without_envelope from msg_mst where seqnum = p_msg_id;
    return v_message_without_envelope;
  exception when others then
    -- email_errors(p_msg_id, 'FWB Message not Found.'||SQLERRM);
    raise_application_error (-20001,'error in the package  - '||sqlerrm);

  end get_message;
-- function that returns the version number
  function get_msg_version(p_msg_id number) return number
  as
  l_msg varchar2(2000);
  l_ver_num number;
  begin
   l_msg := get_message(p_msg_id);
   l_ver_num := to_number(substr(l_msg, 5,1)|| case when regexp_instr(substr(l_msg, 6,1), '[^0-9]') = 0 then substr(l_msg, 6,1) else null end);
   return l_ver_num;
  end get_msg_version;
-- -- Function that checks if the awb exists in awbmst
  function awb_exist_awbmst return number
  as
    l_ubr number;
  begin
    pkg_msg_util.process_log(28400, 'For ME in awb_exist_awbmst r_awb.v_awb_prefix  -->>'|| r_awb.v_awb_prefix );
    pkg_msg_util.process_log(28400, 'For ME in awb_exist_awbmst r_awb.v_awb_no -->>'|| r_awb.v_awb_no );
    select ubr into l_ubr from awbmst 
    where 
        awbpre = r_awb.v_awb_prefix 
    and awbnum = r_awb.v_awb_no;
    
    return l_ubr;
  exception when no_data_found then
    return 0;
  end awb_exist_awbmst;
  
  
-- Function that checks if the House awb exists in awbhse
  function hawb_exist_awbhse(p_hsenum awbhse.hsenum%type) return varchar2
  as
    l_hseq awbhse.hseseq%type;
  begin
    select hseseq into l_hseq from awbhse 
    where 
        awbpre = r_awb.v_awb_prefix 
    and awbnum = r_awb.v_awb_no
    and hsenum = p_hsenum ;

    return l_hseq;
  exception when no_data_found then
    return null;
  end hawb_exist_awbhse;
  
 -- Procedure to generate the hseseq
 procedure generate_hseseq(p_hseseq out number)
  as
    pragma autonomous_transaction;
  begin
    select AWBHSE_SEQ.nextval into p_hseseq from dual;
    commit;
  end generate_hseseq;

 -- Procedure to generate the shpseq
 procedure generate_shpseq(p_shpseq out number)
  as
    pragma autonomous_transaction;
  begin
    select AWBHSESHP_SEQ.nextval into p_shpseq from dual;
    commit;
  end generate_shpseq;
  
-- function is consignment valid
  function is_consignment_valid(
    p_msg_id number,
    p_msg varchar2) return boolean 
  as
    l_awb_prefix  varchar2(3) default null;
    l_hyphen      varchar2(1) default null;
    l_slant       varchar2(1) default null;
    l_awb_number  varchar2(8) default null;
    l_origin      varchar2(3) default null;
    l_destination varchar2(3) default null;
    l_shp_desc    varchar2(1) default null;
    l_pieces      varchar2(4) default null;
    l_weight_code varchar2(1) default null;
    l_weight      varchar2(10) default null;
    l_position    number default 0;
  begin
    pkg_msg_util.process_log(p_msg_id, 'Consignment Details -->>'||p_msg);
    --AWB Prefix
    l_awb_prefix := substr(p_msg,5,3);
    pkg_msg_util.process_log(p_msg_id, 'Consignment Details - l_awb_prefix -->>'|| l_awb_prefix);
    if pkg_msg_util.is_string_number(l_awb_prefix) then
      r_awb.v_awb_prefix := l_awb_prefix;
      l_hyphen := substr(p_msg,8,1);
    else
      pkg_msg_util.process_log(p_msg_id, 'Invalid AWB Prefix. => '|| l_awb_prefix);
      -- email_errors(p_msg_id, 'Invalid AWB Prefix.'||l_awb_prefix); TODO
      return false;
    end if;
    l_log := l_log || l_awb_prefix;
    pkg_msg_util.process_log(p_msg_id, 'Consignment Details - hyphen -->>'|| l_hyphen);
    --Hyphen Separator
    if l_hyphen = msg_hyphen then
      l_awb_number := substr(p_msg,9,8);
    else
      pkg_msg_util.process_log(p_msg_id, 'Hyphen separator missing.');
      -- email_errors(p_msg_id, 'Hyphen separator missing between AWB Prefix and No.'); TODO
      return false;
    end if;

    l_log := l_log || l_hyphen;

    pkg_msg_util.process_log(p_msg_id, 'AWB Number validation.');
    pkg_msg_util.process_log(p_msg_id, 'Consignment Details - l_awb_number -->>'|| l_awb_number);
    --AWB Number validation
    if pkg_msg_util.is_string_number(l_awb_number) then
      r_awb.v_awb_no := l_awb_number;
      l_origin := substr(p_msg,17,3);
      pkg_msg_util.process_log(p_msg_id, 'Consignment Details - l_origin -->>'|| l_origin);
    else
      pkg_msg_util.process_log(p_msg_id, 'Invalid AWB Number.');
      -- email_errors(p_msg_id, 'Invalid AWB Number.'||l_awb_number); TODO
      return false;
    end if;

    l_log := l_log || l_awb_number;

    --AWB Origin validation
    pkg_msg_util.process_log(p_msg_id, 'AWB Origin validation.');
    if pkg_msg_util.is_valid_airport(l_origin) then
      r_awb.v_awb_origin := l_origin;
      l_destination := substr(p_msg,20,3);
      pkg_msg_util.process_log(p_msg_id, 'Consignment Details - l_destination -->>'||l_destination);
    else
      pkg_msg_util.process_log(p_msg_id, 'Invalid AWB Origin Airport.');
      -- email_errors(p_msg_id, 'Invalid AWB Origin Airport.'||l_origin); TODO
      return false;
    end if;

    l_log := l_log || l_origin;

    --AWB Destination validation
    pkg_msg_util.process_log(p_msg_id, 'AWB Destination validation.');
    if pkg_msg_util.is_valid_airport(l_destination) then
      r_awb.v_awb_dest := l_destination;
      l_slant := substr(p_msg,23,1);
    else
      pkg_msg_util.process_log(p_msg_id, 'Invalid AWB Destination Airport.');
      -- email_errors(p_msg_id, 'Invalid AWB Destination Airport.'||l_destination); TODO
      return false;
    end if;

    l_log := l_log || l_destination;

    --Slant Separator
    pkg_msg_util.process_log(p_msg_id, 'Slant Separator.');
    if l_slant = msg_slant then
      l_shp_desc := substr(p_msg,24,1);
    else
      pkg_msg_util.process_log(p_msg_id, 'Slant separator missing after Origin - Destination.');
      -- email_errors(p_msg_id, 'Slant separator missing after Origin - Destination.'); TODO
      return false;
    end if;

    l_log := l_log || l_slant || l_shp_desc ;

    --Shipment Description
    pkg_msg_util.process_log(p_msg_id, 'Shipment Description.');
    if l_shp_desc = msg_shpdes_t then
      --Search for Kilo
      l_position := instr(p_msg, msg_wgtcod_k, 24);

      --Search for Pounds
      if l_position = 0 then
        l_position := instr(p_msg, msg_wgtcod_p, 24);
        if l_position > 0 then 
          r_awb.v_awb_wgtcod := msg_wgtcod_p;
        end if;
      else
        r_awb.v_awb_wgtcod := msg_wgtcod_k;
      end if;

      --Get the Number of Pieces
      if l_position > 0 then 
        l_pieces := substr(p_msg, 25, l_position-25);
        l_log := l_log || l_pieces;
      else
        pkg_msg_util.process_log(p_msg_id, 'Weight Code missing in Consignment Details.');
        -- email_errors(p_msg_id, 'Weight Code missing in Consignment Details.'); TODO
        return false;
      end if;
    else
      pkg_msg_util.process_log(p_msg_id, 'Shipment Description Missing.');
      -- email_errors(p_msg_id, 'Shipment Description Missing.'); TODO
      return false;
    end if;

    pkg_msg_util.process_log(p_msg_id, 'Pieces -->>>'||l_pieces);
    l_log := l_log || 'Position -->>'||l_position;

    --Pieces validation
    l_pieces := rtrim(ltrim(l_pieces));

    if pkg_msg_util.is_string_number(l_pieces) then
      l_log := l_log || 'Pieces = '||l_pieces;
      r_awb.v_awb_pcs := l_pieces;

      l_log := l_log || 'Choose Weight -->>';
      pkg_msg_util.process_log(p_msg_id, 'Choose Weight -->>');

      pkg_msg_util.process_log(p_msg_id, instr(p_msg, msg_volcod_m));

      if instr(p_msg, msg_volcod_m) > 0 then
        pkg_msg_util.process_log(p_msg_id, 'Volume Code is -->>'||msg_volcod_m);
        --l_log := l_log || 'Volume Code is -->>'||msg_volcod_m;
        l_weight := substr(p_msg, l_position + 1, instr(p_msg, msg_volcod_m) - l_position - 1);
      elsif instr(p_msg, msg_volcod_c) > 0 then
        pkg_msg_util.process_log(p_msg_id, 'Volume Code is -->>'||msg_volcod_c);
        l_weight := substr(p_msg, l_position + 1, instr(p_msg, msg_volcod_c) - l_position - 1);
      else
        pkg_msg_util.process_log(p_msg_id, 'Weight is -->>'||replace(substr(p_msg,l_position+1),chr(10)));
        l_weight := replace(substr(p_msg,l_position+1),chr(10));
        l_weight := replace(l_weight, chr(13));
        l_weight := rtrim(ltrim(l_weight));
      end if;

      pkg_msg_util.process_log(p_msg_id, 'Weight is = '||l_weight);
      l_log := l_log || 'Weight is = '||l_weight;
    else
      pkg_msg_util.process_log(p_msg_id, 'Invalid Number of Pieces.');
      -- email_errors(p_msg_id, 'Invalid Number of Pieces.'||l_pieces); TODO
      return false;
    end if;

    pkg_msg_util.process_log(p_msg_id, 'Weight -->>>'||l_weight);

    --Weight validation [Assumption Optional Volume missing]
    l_log := l_log || l_weight;
    --if pkg_msg_util.is_string_number(abs(l_weight)) then
      --r_awb.v_awb_wgt := substr(l_weight,1,length(l_weight)-1);
      r_awb.v_awb_wgt := l_weight;
    --else
      --pkg_msg_util.process_log(p_msg_id, 'Invalid Weight.');
      --email_errors(p_msg_id, 'Invalid Weight.'||l_weight);
      --return false;
    --end if;

    l_log := l_log || 'WGT -->>' || r_awb.v_awb_wgt;
    return true;
  exception when others then
    pkg_msg_util.process_log(p_msg_id, 'Exception in CONSIGNMENT Details -->>'||SQLERRM);
    -- email_errors(p_msg_id, 'Consignment Details.'||p_msg||SQLERRM); TODO
    return false;
  end is_consignment_valid;
  
-- function is consignee valid 
function is_shipper_valid(
    p_msg_id number,
    p_msg varchar2) return boolean 
  as
    l_shp_name      VARCHAR2(100);
    l_shp_address   VARCHAR2(100);
    l_shp_place     VARCHAR2(100);
    l_shp_prov      VARCHAR2(100);
    l_shp_cnt_code  VARCHAR2(2);
    l_shp_postal    VARCHAR2(15);
    l_line          varchar2(1000);
    l_position      number;
  begin
    l_log := l_log || 'Shipper Details -->>'|| p_msg;
    pkg_msg_util.process_log(p_msg_id, 'Shipper Details -->>'||p_msg);
    --Shipper Name
    if msg_version = 5 then 
      Begin
        if substr(p_msg,1,3) = msg_ldr_shp then
          l_shp_name := pkg_msg_util.split_string_line(p_msg,2);
          pkg_msg_util.process_log(p_msg_id, 'Shipper  Details - l_shp_name -->>'||l_shp_name);
          l_log := l_log || l_shp_name;
          if substr(l_shp_name,4,1) = msg_slant then
            l_shp_name := substr(l_shp_name,5,35);
            r_awb.v_shp_name := l_shp_name;
          else
            pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Name.');
            l_log := l_log || 'Invalid Shipper Name. -->>'||l_shp_name;
            return false;
          end if;
        end if;
    
        --Shipper Address
        l_shp_address := pkg_msg_util.split_string_line(p_msg,3);
        pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_address -->>'||l_shp_address);
        l_log := l_log || l_shp_address;
        if substr(l_shp_address,4,1) = msg_slant then
          l_shp_address := substr(l_shp_address,5,35);
          r_awb.v_shp_address := l_shp_address;
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Address.');
          l_log := l_log || 'Invalid Shipper Address. -->>'||l_shp_address;
          return false;
        end if;
    
        --Shipper Place and Provice
        l_line := pkg_msg_util.split_string_line(p_msg,4);
        pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_line -->>'||l_line);
    
        l_log := l_log || l_line;
        if substr(l_line,4,1) = msg_slant then
          l_shp_place := substr(l_line,5,17);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_place -->>'||l_shp_place);
    
          --Does Province exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_shp_place := substr(substr(l_line,2,l_position-2),1,17);
              l_shp_prov := substr(l_line,l_position+1,9);
            end if;
          end if;
          r_awb.v_shp_place := l_shp_place;
          r_awb.v_shp_prov := l_shp_prov;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_prov -->>'||l_shp_prov);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Place and Province.');
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_prov -->>'||l_shp_prov);
          l_log := l_log || 'Invalid Shipper Place and Province. -->>'||l_line;
          return false;
        end if;
        
        --Shipper Country and Postal Code
        l_line := pkg_msg_util.split_string_line(p_msg,5);
        l_log := l_log || l_line;
        if substr(l_line,1,1) = msg_slant then
          l_shp_cnt_code := substr(l_line,2,2);
          --Does Postal Code exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_shp_postal := substr(l_line,l_position+1,9);
              if instr(l_shp_postal,'/') > 0 then
                l_shp_postal := substr(l_shp_postal,1, instr(l_shp_postal,'/')-1);
                
                
              end if;
            end if;
          end if;
          r_awb.v_shp_cnt_code := l_shp_cnt_code;
          r_awb.v_shp_postal := l_shp_postal;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_cnt_code -->>'||l_shp_cnt_code);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_postal -->>'||l_shp_postal);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Country and Postal Code.');
          l_log := l_log || 'Invalid Shipper Country and Postal Code. -->>'||l_line;
          return false;
        end if;
      end; 
     else -- Version is either 2 or 4 
       Begin
        if substr(p_msg,1,3) = msg_ldr_shp then
           -- l_shp_name := pkg_msg_util.split_string_line(p_msg,1);
           l_shp_name := substr(p_msg, instr(p_msg, msg_slant), (instr(p_msg, msg_crlf) - instr(p_msg, msg_slant)));
           pkg_msg_util.process_log(p_msg_id, 'Shipper  Details - l_shp_name -->>'||l_shp_name);
           l_log := l_log || l_shp_name;
          if substr(l_shp_name,1,1) = msg_slant then
          -- if substr(p_msg,4,1) = msg_slant then
            l_shp_name := substr(l_shp_name,2,35);
            --l_log := l_log || l_shp_name;
            r_awb.v_shp_name := l_shp_name;
          else
            pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Name.');
            l_log := l_log || 'Invalid Shipper Name. -->>'||l_shp_name;
            return false;
          end if;
        end if;
    
        --Shipper Address
        l_shp_address := pkg_msg_util.split_string_line(p_msg,2);
        pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_address -->>'||l_shp_address);
        l_log := l_log || l_shp_address;
        if substr(l_shp_address,1,1) = msg_slant then
          l_shp_address := substr(l_shp_address,2,35);
          r_awb.v_shp_address := l_shp_address;
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Address.');
          l_log := l_log || 'Invalid Shipper Address. -->>'||l_shp_address;
          return false;
        end if;
    
        --Shipper Place and Provice
        l_line := pkg_msg_util.split_string_line(p_msg,3);
        pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_line -->>'||l_line);
    
        l_log := l_log || l_line;
        if substr(l_line,1,1) = msg_slant then
          l_shp_place := substr(l_line,3,17);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_place -->>'||l_shp_place);
    
          --Does Province exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_shp_place := substr(substr(l_line,2,l_position-2),1,17);
              l_shp_prov := substr(l_line,l_position+1,9);
            end if;
          end if;
          r_awb.v_shp_place := l_shp_place;
          r_awb.v_shp_prov := l_shp_prov;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_prov -->>'||l_shp_prov);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Place and Province.');
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_prov -->>'||l_shp_prov);
          l_log := l_log || 'Invalid Shipper Place and Province. -->>'||l_line;
          return false;
        end if;
        
        --Shipper Country and Postal Code
        l_line := pkg_msg_util.split_string_line(p_msg,4);
        l_log := l_log || l_line;
        if substr(l_line,1,1) = msg_slant then
          l_shp_cnt_code := substr(l_line,2,2);
          --Does Postal Code exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_shp_postal := substr(l_line,l_position+1,9);
              if instr(l_shp_postal,'/') > 0 then
                l_shp_postal := substr(l_shp_postal,1, instr(l_shp_postal,'/')-1);
                
                
              end if;
            end if;
          end if;
          r_awb.v_shp_cnt_code := l_shp_cnt_code;
          r_awb.v_shp_postal := l_shp_postal;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_cnt_code -->>'||l_shp_cnt_code);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_shp_postal -->>'||l_shp_postal);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Shipper Country and Postal Code.');
          l_log := l_log || 'Invalid Shipper Country and Postal Code. -->>'||l_line;
          return false;
        end if;
      end; 
    end if;
    
    return true;
  exception when others then
    l_log := l_log || 'Error Processing Shipper. -->>'||SQLERRM;
    return false;
  end is_shipper_valid;
  
-- function is consignee valid  
function is_consignee_valid(
    p_msg_id number,
    p_msg varchar2) return boolean 
  as
    l_con_name      VARCHAR2(100);
    l_con_address   VARCHAR2(100);
    l_con_place     VARCHAR2(100);
    l_con_prov      VARCHAR2(100);
    l_con_cnt_code  VARCHAR2(100);
    l_con_postal    VARCHAR2(100);
    l_line          varchar2(100);
    l_position      number;
  begin
    if msg_version = 5 then
    Begin
        --Consignee Name
        if substr(p_msg,1,3) = msg_ldr_cne then
          l_con_name := pkg_msg_util.split_string_line(p_msg,2);
           l_log := l_log || l_con_name;
          if substr(l_con_name,4,1) = msg_slant then
            l_con_name := substr(l_con_name,5);
            r_awb.v_con_name := l_con_name;
            pkg_msg_util.process_log(p_msg_id, 'Consignee Name=> '|| l_con_name);
          else
            pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Name.');
             l_log := l_log || 'Invalid Consignee Name. -->>'||l_con_name;
            return false;
          end if;
        end if;
    
        --Consignee Address
        l_con_address := pkg_msg_util.split_string_line(p_msg,3);
        pkg_msg_util.process_log(p_msg_id, 'Consignee Address  => '|| l_con_address);
        l_log := l_log || l_con_address;
        if substr(l_con_address,4,1) = msg_slant then
          l_con_address := substr(l_con_address,5);
          r_awb.v_con_address := l_con_address;
          -- pkg_msg_util.process_log(p_msg_id, 'Consignee Address=> '|| l_con_address);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Address.');
          return false;
        end if;
    
        --Shipper Place and Provice
        l_line := pkg_msg_util.split_string_line(p_msg,4);
        -- pkg_msg_util.process_log(p_msg_id, 'Consignee Address=> l_line '|| l_line);
        l_log := l_log || l_line;
        if substr(l_line,4,1) = msg_slant then
          l_con_place := substr(l_line,5,17);
          --Does Province exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_con_place := substr(substr(l_line,2,l_position-2),1,17);
              l_con_prov := substr(l_line,l_position+1,9);
            end if;
          end if;
          r_awb.v_con_place := l_con_place;
          r_awb.v_con_prov := l_con_prov;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_place -->>'||l_con_place);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_prov -->>'||l_con_prov);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Place and Province.');
           l_log := l_log || 'Invalid Consignee Place and Province. -->>';
          return false;
        end if;
        
        --Shipper Country and Postal Code
        l_line := pkg_msg_util.split_string_line(p_msg,5);
        pkg_msg_util.process_log(p_msg_id, 'Consignee Address=> l_line '|| l_line);
        l_log := l_log || l_line;
        if substr(l_line,1,1) = msg_slant then
          l_con_cnt_code := substr(l_line,2,2);
          --Does Postal Code exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_con_postal := substr(l_line,l_position+1,9);
              if instr(l_con_postal,'/') > 0 then
                l_con_postal := substr(l_con_postal,1, instr(l_con_postal,'/')-1);
              end if;
            end if;
          end if;
          r_awb.v_con_cnt_code := l_con_cnt_code;
          r_awb.v_con_postal := l_con_postal;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_postal -->>'||l_con_postal);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_cnt_code -->>'||l_con_cnt_code);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Country and Postal Code.');
          l_log := l_log || 'Invalid Consignee Country and Postal Code. -->>';
          return false;
        end if;             
      end;
      else -- version is either 2 or 4
        Begin
        --Consignee Name
        if substr(p_msg,1,3) = msg_ldr_cne then
          l_con_name := substr(p_msg, instr(p_msg, msg_slant), (instr(p_msg, msg_crlf) - instr(p_msg, msg_slant)));
          pkg_msg_util.process_log(p_msg_id, ' for me Shipper  Details - l_con_name -->>'||l_con_name);
           l_log := l_log || l_con_name;
          if substr(l_con_name,1,1) = msg_slant then
            l_con_name := substr(l_con_name,2);
            r_awb.v_con_name := l_con_name;
            pkg_msg_util.process_log(p_msg_id, 'Consignee Name=> '|| l_con_name);
          else
            pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Name.');
             l_log := l_log || 'Invalid Consignee Name. -->>'||l_con_name;
            return false;
          end if;
        end if;
        
        --Consignee Address
        l_con_address := pkg_msg_util.split_string_line(p_msg,2);
        pkg_msg_util.process_log(p_msg_id, 'Consignee Address  => '|| l_con_address);
        l_log := l_log || l_con_address;
        if substr(l_con_address,1,1) = msg_slant then
          l_con_address := substr(l_con_address,2);
          r_awb.v_con_address := l_con_address;
          -- pkg_msg_util.process_log(p_msg_id, 'Consignee Address=> '|| l_con_address);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Address.');
          return false;
        end if;
    
        -- Consigne Place and Provice
        l_line := pkg_msg_util.split_string_line(p_msg,3);
        -- pkg_msg_util.process_log(p_msg_id, 'Consignee Address=> l_line '|| l_line);
        l_log := l_log || l_line;
        if substr(l_line,1,1) = msg_slant then
          l_con_place := substr(l_line,2,17);
          --Does Province exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_con_place := substr(substr(l_line,2,l_position-2),1,17);
              l_con_prov := substr(l_line,l_position+1,9);
            end if;
          end if;
          r_awb.v_con_place := l_con_place;
          r_awb.v_con_prov := l_con_prov;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_place -->>'||l_con_place);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_prov -->>'||l_con_prov);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Place and Province.');
           l_log := l_log || 'Invalid Consignee Place and Province. -->>';
          return false;
        end if;
        -- Consignee Country and Postal Code
        l_line := pkg_msg_util.split_string_line(p_msg,4);
        pkg_msg_util.process_log(p_msg_id, 'Consignee Address=> l_line '|| l_line);
        l_log := l_log || l_line;
        if substr(l_line,1,1) = msg_slant then
          l_con_cnt_code := substr(l_line,2,2);
          --Does Postal Code exist
          if instr(l_line, msg_slant, 1, 2) > 0 then
            l_position := instr(l_line, msg_slant, 1, 2);
            if l_position > 0 then
              l_con_postal := substr(l_line,l_position+1,9);
              if instr(l_con_postal,'/') > 0 then
                l_con_postal := substr(l_con_postal,1, instr(l_con_postal,'/')-1);
              end if;
            end if;
          end if;
          r_awb.v_con_cnt_code := l_con_cnt_code;
          r_awb.v_con_postal := l_con_postal;
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_postal -->>'||l_con_postal);
          pkg_msg_util.process_log(p_msg_id, 'Shipper Details - l_con_cnt_code -->>'||l_con_cnt_code);
        else
          pkg_msg_util.process_log(p_msg_id, 'Invalid Consignee Country and Postal Code.');
          l_log := l_log || 'Invalid Consignee Country and Postal Code. -->>';
          return false;
        end if;             
      end;
    end if;  
    return true;
  exception when others then
    l_log := l_log || 'Error Processing Consignee. -->>'||SQLERRM;
    return false;
  end is_consignee_valid;
-- Procedure to create a new house AWB
procedure create_hawb(p_msg_id number, p_hsenum awbhse.hsenum%type, p_hseq out number,re_awbhseshp rec_awbhseshp)
  as
    l_hseq awbhse.hseseq%type;
    l_shpseq AWBHSESHP.shpseq%type;

  begin
    pkg_msg_util.process_log(p_msg_id, ' For ME re_awbhseshp values. v_shpwgt => ' || re_awbhseshp.v_shpwgt);
    pkg_msg_util.process_log(p_msg_id, ' For ME re_awbhseshp values. v_shppcs => ' || re_awbhseshp.v_shppcs);
    generate_hseseq(l_hseq);
    pkg_msg_util.process_log(p_msg_id, 'For ME inside  AWBHSE l_hseq -->>' || l_hseq);
    insert into awbhse (hseseq, ubr, awbpre, awbnum, hsenum, shpnam, shpadd, connam, conadd, remark
                      , shpcty,shppro,shpcou,shppos,concty,conpro,concou,conpos)
      values(l_hseq, r_awb.v_awb_ubr,r_awb.v_awb_prefix, r_awb.v_awb_no, p_hsenum, r_awb.v_shp_name, r_awb.v_shp_address,r_awb.v_con_name,r_awb.v_con_address, r_awb.v_remark
           , r_awb.v_shp_place, r_awb.v_shp_prov, r_awb.v_shp_cnt_code, r_awb.v_shp_postal,r_awb.v_con_place, r_awb.v_con_prov, r_awb.v_con_cnt_code, r_awb.v_con_postal );
      generate_shpseq(l_shpseq);
      -- insert in the AWBHSESHP detail table
      pkg_msg_util.process_log(p_msg_id, ' For ME Detail house AWB created ');
      insert into awbhseshp (hseseq,shpseq,comcod,shppcs,shpwgt) 
      values(l_hseq,l_shpseq, null, re_awbhseshp.v_shppcs, re_awbhseshp.v_shpwgt);
    p_hseq := l_hseq;
  exception when others then
    pkg_msg_util.process_log(p_msg_id, 'House AWB Insert Failed.'||SQLERRM);
    -- email_errors(p_msg_id, SQLERRM); TODO
    p_hseq := 0;
  end create_hawb;
  
-- Process the decoded FHL message:
  procedure spr_process_fhl(p_msg_id number)
  as
    l_ubr number default 0;
    l_hsenum awbhse.hsenum%type;
    l_hseq   awbhse.hseseq%type;
    l_fhl_msg varchar2(2000);
    l_msg1     varchar2(800);
    re_awbhseshp rec_awbhseshp;
    l_pieces_positions  number;
    l_weight_positions  number;
  begin
    -- Get msg_version
    msg_version := get_msg_version(p_msg_id);
    pkg_msg_util.process_log(p_msg_id, 'For ME version number -->>'||msg_version);
    l_fhl_msg := get_message(p_msg_id);
    pkg_msg_util.process_log(p_msg_id, 'For ME Start in process p_msg_id -->>'||substr(l_fhl_msg,1, 20));
    -- Check if the master AWB exists
    l_ubr := awb_exist_awbmst;
    pkg_msg_util.process_log(p_msg_id, 'For ME Start in process l_ubr -->>'||l_ubr);
    if l_ubr > 0 then
      --If Yes then check if the House AWB exist in AWBHSE
      r_awb.v_awb_ubr := l_ubr;
      -- Extract all lines for HBS from the message
      -- l_msg1 := pkg_msg_util.split_string_between(l_fhl_msg, 'MBI', 'SHP');
      l_fhl_msg := replace(l_fhl_msg, 'HBS/', '@/');
      -- insert into tr_hh values(l_msg1,null,null,null);
      for hbs in ( select REGEXP_SUBSTR(l_fhl_msg, '@[^@]*', 1, level) as reg_exp 
                     from dual
                      connect by REGEXP_SUBSTR (l_fhl_msg,'@[^@]*', 1, level) is not null) 
        loop
          -- pkg_msg_util.process_log(p_msg_id, 'For ME hbs.reg_exp -->>'||hbs.reg_exp);
          select ltrim(REGEXP_SUBSTR(hbs.reg_exp, '/[^/]*'),'/') into l_hsenum from dual;
          l_hseq := hawb_exist_awbhse(l_hsenum);
          -- pkg_msg_util.process_log(p_msg_id, 'For ME hbs.reg_exp -->>'||hbs.reg_exp);
          pkg_msg_util.process_log(p_msg_id, 'For ME l_hsenum -->>'||l_hsenum);
          pkg_msg_util.process_log(p_msg_id, 'For ME l_hseq-->>'||l_hseq);
          if l_hseq is not null then
            pkg_msg_util.process_log(p_msg_id, 'House AWB  already exists. Cannot modify. HSENUM is -->>'||l_hsenum);
            -- email_errors(p_msg_id, 'Booking already exists. Cannot modify. '||l_ubr); TODO
           else
           -- read data from each HBS line
            
            select length(substr(hbs.reg_exp,INSTR(hbs.reg_exp , msg_slant, 1, 3)))- length(substr(hbs.reg_exp,INSTR(hbs.reg_exp , msg_slant, 1, 4))) -1  into l_pieces_positions from dual;
            select length(substr(hbs.reg_exp,INSTR(hbs.reg_exp , msg_slant, 1, 4)))- length(substr(hbs.reg_exp,INSTR(hbs.reg_exp , msg_slant, 1, 5))) -2  into l_weight_positions from dual; 
            re_awbhseshp.v_shppcs := substr(hbs.reg_exp,INSTR(hbs.reg_exp , msg_slant, 1, 3)+1, l_pieces_positions);
            re_awbhseshp.v_shpwgt := substr(hbs.reg_exp,INSTR(hbs.reg_exp , msg_slant, 1, 4)+2, l_weight_positions);
            --r_awb.v_remark := substr(hbs.reg_exp,INSTR(hbs.reg_exp , msg_slant, 1, 6)+1, 20);
            r_awb.v_remark := substr(hbs.reg_exp, instr(hbs.reg_exp,msg_slant,1,6)+1, (instr(hbs.reg_exp,msg_crlf,1) - instr(hbs.reg_exp,msg_slant,1,6)));
            pkg_msg_util.process_log(p_msg_id, 'For ME r_awb.v_remark -->>'||r_awb.v_remark);
            pkg_msg_util.process_log(p_msg_id, 'For ME call to proc create awbhouse -->>'||l_hseq);
            pkg_msg_util.process_log(p_msg_id, 'For ME l_weight_positions -->>'||l_weight_positions);
            pkg_msg_util.process_log(p_msg_id, 'For ME l_pieces_positions -->>'||l_pieces_positions);
            create_hawb(p_msg_id, l_hsenum, l_hseq, re_awbhseshp);
            pkg_msg_util.process_log(p_msg_id, 'For ME Call to create AWBHSE-->>');
            
            if l_hseq > 0 then
              pkg_msg_util.process_log(p_msg_id, 'House AWB Created.');
             else
              pkg_msg_util.process_log(p_msg_id, 'House AWB insert Failed.');
            end if;  
          end if;  
        end loop; 
    else
      pkg_msg_util.process_log(p_msg_id, 'Booking does not exist for this AWB.');
    end if;

    -- create_bkg_dtl(l_ubr, p_msg_id); TODO
        
  exception when others then
    pkg_msg_util.process_log(p_msg_id, 'FHL Processing Failed.' || sqlerrm);
    -- email_errors(p_msg_id, 'FWB Processing Failed.'||SQLERRM); TODO
  end spr_process_fhl;
 
 -- Get HBS lines
 
 function extract_hbs_lines(l_fhl_msg varchar2) return varchar2
 is
  
 begin
    return null;
 end;
  -- Procedure deode FHL message  
  PROCEDURE decode_fhl(
    p_msg_id IN number )
  as

    l_fhl_msg varchar2(2000);
    l_msg varchar2(500);
  begin
    l_fhl_msg := get_message(p_msg_id);
      --Extract the Consignment Details.
    if is_consignment_valid(p_msg_id, pkg_msg_util.split_string_line(l_fhl_msg, 2)) then
      pkg_msg_util.process_log(p_msg_id, 'Valid Consignment Details.');
    else
      pkg_msg_util.process_log(p_msg_id, 'InValid Consignment Details.');
      -- email_errors(p_msg_id, 'InValid Consignment Details.'); TODO
    end if;
--Extract the Shipper Details.
    if is_shipper_valid(p_msg_id, pkg_msg_util.split_string_between(l_fhl_msg, 'SHP', 'CNE')) then
      pkg_msg_util.process_log(p_msg_id, 'Valid Shipper Details.');
    else
      pkg_msg_util.process_log(p_msg_id, 'InValid Shipper Details.');
      -- email_errors(p_msg_id, 'InValid Shipper Details.'); TODO
    end if;
--Extract the Consignee Details.
    l_msg := pkg_msg_util.split_string_between(l_fhl_msg, 'CNE', 'CVD/');
    if length(l_msg) > 0 then
      if is_consignee_valid(p_msg_id, l_msg) then
        pkg_msg_util.process_log(p_msg_id, 'Valid Consignee Details.');
      else
        pkg_msg_util.process_log(p_msg_id, 'InValid Consignee Details.');
        -- email_errors(p_msg_id, 'InValid Consignee Details.'); TODO
      end if;
    else
      pkg_msg_util.process_log(p_msg_id, 'Consignee Details Not Found.');
      -- email_errors(p_msg_id, 'Consignee Details Not Found.'); TODO
    end if;
    -- Message is decoded, call the process 
     spr_process_fhl(p_msg_id);
  exception when others then
    -- email_errors(p_msg_id, 'FHL Decoding Failed.'||SQLERRM); 
    null; -- TODO 
  end decode_fhl;  

end pkg_process_fhl;
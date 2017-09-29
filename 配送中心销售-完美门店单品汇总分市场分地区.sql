       select  hz.preorgcode,
            hz.preorgname,
            hz.zbcode,
            hz.plucode,
            hz.pluname,
            hz.materialcode,
            hz.cargono,
            hz.plumodel,
            hz.postcode,
            hz.clscode,
            hz.clsname,
            count(distinct hz.preorgcode) FgsCounts,
            count(distinct hz.PFSaleNo) PFSaleCounts, 
            count(distinct hz.CUSTCODE) cutcounts,
            count(distinct hz.Rzdate) RzCounts,
            count(distinct hz.PluCode) PluCounts,
            sum(hz.Counts) Totalcounts,
            sum(hz.ystotal) Totalys,
            sum(hz.PFTotal) PFTotal
        from
        (Select g.preorgcode,
               (select orgname from tOrgDrpRelation where ORGCODE = g.preorgcode) preorgname,
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               i.CUSTCODE || i.ORGCODE CUSTCODE, 
               f.Rzdate || i.CUSTCODE || i.ORGCODE Rzdate,
               f.PFSaleNo,
               en.postcode,
               d.PluCode, 
               t.pluname, 
               t.MATERIALCODE,
               t.CARGONO,
               t.PLUMODEL,
               p.clscode, 
               p.clsname, 
               d.Counts,
               d.ystotal,
               d.PFTotal
          From tSkuplu                  t,
               tCatCategory              p, 
               torgdrpdisp               h,  
               tOrgDrpRelation           g,
               tDrpEtpCustform           i, 
               tDrpEtpCustdetail         e,
               tmgpfsaleform_rpt@hsrpt   f,
               tmgpfsaledetail_rpt@hsrpt d,
               tEtpEnterprise            en
         Where f.PFSaleNo = d.pfsaleno and f.branchno = d.branchno 
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode 
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid  
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%有效%' 
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1' 
           and f.branchno = en.etpcode
           and f.Rzdate between '2017-06-22' and '2017-09-22'
        union all
        Select g.preorgcode,
               (select orgname from tOrgDrpRelation where ORGCODE = g.preorgcode) preorgname,
               (select preorgcode from tOrgDrpRelation where ORGCODE = g.preorgcode) Zbcode,   
               '' as CUSTCODE, 
               '' as Rzdate,
               '' as PFSaleNo,
               en.postcode,
               d.PluCode, 
               t.pluname, 
               t.MATERIALCODE,
               t.CARGONO,
               t.PLUMODEL,
               p.clscode, 
               p.clsname, 
               -d.Counts      as Counts,
               -d.yttotal     as ystotal,
               -d.PFTotal     as PFTotal
          From tSkuplu                    t,
               tCatCategory                p, 
               torgdrpdisp                 h, 
               tOrgDrpRelation             g, 
               tDrpEtpCustform             i, 
               tDrpEtpCustdetail           e,
               tmgpfreturnform_rpt@hsrpt   f,
               tmgpfreturndetail_rpt@hsrpt d,
               tEtpEnterprise              en
         Where f.PFreturnNo = d.pfreturnno and f.branchno = d.branchno
           and e.Orgcode = f.Branchno and e.CustCode = f.CustomerCode  
           and i.Orgcode = g.Orgcode 
           and e.kid = i.kid 
           and f.branchno = h.orgcode 
           and d.plucode = t.plucode  
           and t.clsid = p.clsid 
           and substr(p.clscode,1,1) = '0' 
           and h.remark like '%有效%'
           and e.ISPERFECT = '1' 
           and e.isxuesheng = '1'
           and f.branchno = en.etpcode
           and f.Rzdate between '2017-06-22' and '2017-09-22') hz 
        group by hz.preorgcode, hz.preorgname, hz.zbcode, hz.plucode, 
        hz.pluname, hz.materialcode, hz.cargono, hz.plumodel, 
        hz.postcode, hz.clscode, hz.clsname;
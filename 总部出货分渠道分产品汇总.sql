Select Case When s.orgcode In('0089','0088') Then '��������'         --����
            When s.orgcode In('018','020') Then '��Ʒ����'  --����ݡ�������
            Else '��ͳ����' End As ����,
       p.udp14 As Ʒ��,
       p.plucode,
       p.materialcode,
       sum(s.pstotal)/10000 As ���
  From (
Select h.shorgcode As orgcode,b.plucode,b.pstotal
  From tdstpsbody b,tdstpshead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '2016-01-01' And '2016-12-31'
   And h.orgcode In('1','2') And h.shorgcode In('0089','018','020','021','023','033','034','035','036','037','048','054','055','0088','0852')
Union All
Select h.thorgcode As orgcode,b.plucode,-b.thtotal As pstotal
  From tdstrtnbody b,tdstrtnhead h
 Where b.billno = h.billno
   And to_char(h.jzdate,'yyyy-mm-dd') Between '2016-01-01' And '2016-12-31'
   And h.orgcode In('1','2') And h.thorgcode In('0089','018','020','021','023','033','034','035','036','037','048','054','055','0088','0852')
       ) s,tskuplu p
 Where s.plucode = p.plucode
   And Exists(Select 1 From tcatcategory Where clsid = p.clsid And clscode Like '0%' And clscode Not Like '011%')  --������
 Group By Case When s.orgcode In('0089','0088') Then '��������' When s.orgcode In('018','020') Then '��Ʒ����' Else '��ͳ����' End, s.orgcode, '0089', '0088', '��������', 
s.orgcode, '018', '020', '��Ʒ����', '��ͳ����', 
p.udp14, p.plucode, p.materialcode
 Order By Case When s.orgcode In('0089','0088') Then '��������' When s.orgcode In('018','020') Then '��Ʒ����' Else '��ͳ����' End,p.udp14

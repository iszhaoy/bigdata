create 'student','info',{NAME=>'info',VERSIONS=>3}

alter 'student',{NAME=>'info',VERSIONS=>3}

get 'student','1001',{COLUMN=>'info:name',VERSIONS=>3}


put 'student','1001','info:sex','male'
put 'student','1001','info:age','18'
put 'student','1002','info:name','mJanna'
put 'student','1002','info:sex','famale'
put 'student','1002','info:age','20'

scan 'student'

scan 'student',{STARTROW => '1001',STOPROW => '1001'}
scan 'student',{STARTROW => '1001'}

get 'student','1001'

-- 统计表行数
count 'student'

deleteall 'student','1001'

delete 'student','1002','info:sex'

truncate 'student'

--提示：清空表的操作顺序为先 disable，然后再 truncate。
disable 'student'
drop 'student'


-- 预分区
-- 手动预设分区
create	'staff1','info','partition1',SPLITS => ['1000','2000','3000','4000']

create 'staff2','info',{NUMREGIONS => 15, SPLITALGO => 'HexStringSplit'}


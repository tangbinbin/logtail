create database if not exists log default charset=utf8;

use log;
create table if not exists log(
	id bigint unsigned not null auto_increment,
	host varchar(40) not null default '',
	filename varchar(100) not null default '',
	msg varchar(2000) not null default '',
	primary key(id)
)engine=innodb default charset=utf8;

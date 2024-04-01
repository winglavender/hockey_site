ALTER TABLE teammates MODIFY playerId_x VARCHAR(20);
alter table teammates ADD INDEX (playerId_x);
ALTER TABLE teammates MODIFY playerId_y VARCHAR(20);
alter table teammates ADD INDEX (playerId_y);
ALTER TABLE teammates MODIFY team VARCHAR(100);
alter table teammates ADD INDEX (team);
ALTER TABLE teammates MODIFY overlap_start_date datetime;
alter table teammates ADD INDEX (overlap_start_date);

ALTER TABLE skaters MODIFY playerId VARCHAR(20);
alter table skaters add index (playerId);
ALTER TABLE skaters MODIFY start_date datetime;
alter table skaters add index (start_date);
ALTER TABLE skaters MODIFY end_date datetime;
alter table skaters add index (end_date);
ALTER TABLE skaters MODIFY league VARCHAR(50);
alter table skaters add index (league);
ALTER TABLE skaters MODIFY team VARCHAR(20);
alter table skaters add index (team);

ALTER TABLE player_playoffs MODIFY link VARCHAR(255);
alter table player_playoffs add index (link);

ALTER TABLE links MODIFY playerId VARCHAR(20);
alter table links add index (playerId);
ALTER TABLE links MODIFY playerName VARCHAR(255);
alter table links add index (playerName);

ALTER TABLE player_names MODIFY playerName VARCHAR(255);
alter table player_names add index (playerName);

ALTER TABLE norm_names MODIFY playerName VARCHAR(255);
alter table norm_names add index (playerName);
ALTER TABLE norm_names MODIFY norm_name VARCHAR(255);
alter table norm_names add index (norm_name);

ALTER TABLE game_player MODIFY gameId VARCHAR(20);
alter table game_player add index (gameId);
ALTER TABLE game_player MODIFY playerId VARCHAR(20);
alter table game_player add index (playerId);
ALTER TABLE game_player MODIFY team VARCHAR(20);
alter table game_player add index (team);

ALTER TABLE games MODIFY gameId VARCHAR(20);
alter table games add index (gameId);
ALTER TABLE games MODIFY gameDate datetime;
alter table games add index (gameDate);
ALTER TABLE games MODIFY winningTeam VARCHAR(20);
alter table games add index (winningTeam);
ALTER TABLE games MODIFY awayTeam VARCHAR(20);
alter table games add index (awayTeam);
ALTER TABLE games MODIFY homeTeam VARCHAR(20);
alter table games add index (homeTeam);

ALTER TABLE scratches MODIFY gameId VARCHAR(20);
alter table scratches add index (gameId);
ALTER TABLE scratches MODIFY playerId VARCHAR(20);
alter table scratches add index (playerId);
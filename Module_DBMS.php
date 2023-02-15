<?php
namespace GDO\DBMS;

use GDO\Core\GDO_Module;
use GDO\Core\GDO;
use GDO\DB\Database;

/**
 * MySQLi DBMS module.
 * *NEW* This is a required multi-provider module.
 * Minimalistic API to support a DBMS.
 * Configuration is done in protected/config.php
 * 
 * @author gizmore
 * @version 7.0.2
 * @since 7.0.2
 */
final class Module_DBMS extends GDO_Module
{
	public int $priority = 7;
	
	private \mysqli $link;

	##############
	### Module ###
	##############
	public function checkSystemDependencies() : bool
	{
		if (!function_exists('mysqli_query'))
		{
			return $this->errorSystemDependency('err_php_extension', ['mysqli']);
		}
		return true;
	}
	
	################
	### DBMS API ###
	################
	public function dbmsOpen(string $host, string $user, string $pass, string $database=null, int $port=3306): \mysqli
	{
		$this->link = mysqli_connect($host, $user, $pass, $database, $port);
		$this->dbmsQuery("SET NAMES UTF8");
		$this->dbmsQuery("SET time_zone = '+00:00'");
		return $this->link;
	}
	
	public function dbmsClose(): void
	{
		mysqli_close($this->link);
	}
	
	public function dbmsForeignKeys(bool $foreignKeysEnabled): void
	{
		$check = (int)$foreignKeysEnabled;
		$this->dbmsQry("SET foreign_key_checks = {$check}");
	}
	
	public function dbmsQry(string $query)
	{
		return $this->dbmsQuery($query, false);
	}
	
	public function dbmsQuery(string $query, bool $buffered=true)
	{
		return mysqli_query($this->link, $query);
	}
	
	public function dbmsFree(\mysqli_result $result): void
	{
		mysqli_free_result($result);
	}
	
	public function dbmsFetchRow(\mysqli_result $result): ?array
	{
		return mysqli_fetch_row($result);
	}
	
	public function dbmsFetchAll(\mysqli_result $result): ?array
	{
		return mysqli_fetch_all($result);
	}
	
	public function dbmsFetchAssoc(\mysqli_result $result): ?array
	{
		return mysqli_fetch_assoc($result);
	}
	
	public function dbmsNumRows($result): int
	{
		return mysqli_num_rows($result);
	}
	
	public function dbmsInsertId(): int
	{
		return mysqli_insert_id($this->link);
	}
	
	public function dbmsAffected(): int
	{
		return mysqli_affected_rows($this->link);
	}
	
	public function dbmsBegin(): void
	{
		mysqli_begin_transaction($this->link);
	}
	
	public function dbmsCommit(): void
	{
		mysqli_commit($this->link);
	}
	
	public function dbmsRollback(): void
	{
		mysqli_rollback($this->link);
	}
	
	public function dbmsLock(string $lock, int $timeout=30): void
	{
		$query = "SELECT GET_LOCK('{$lock}', {$timeout}) as L";
		$this->dbmsQuery($query, false);
	}
	
	public function dbmsUnlock(string $lock): void
	{
		$query = "SELECT RELEASE_LOCK('{$lock}') AS L";
		$this->dbmsQuery($query, false);
	}
	
	public function dbmsError(): string
	{
		return mysqli_error($this->link);
	}
	
	public function dbmsErrno(): int
	{
		return mysqli_errno($this->link);
	}
	
	############
	### Bulk ###
	############
	public function dbmsExecFile(string $path): void
	{
		$fh = fopen($path, 'r');
		$command = '';
		while ($line = fgets($fh))
		{
			$line = trim($line);
			
			if ( (str_starts_with($line, '-- ')) ||
				(str_starts_with($line, '/*')) )
			{
				# skip comments
				continue;
			}
			
			# Append to command
			$command .= $line;
			
			# Finished command
			if (str_ends_with($line, ';'))
			{
				# Most likely a write
				$this->dbmsQry($command);
				$command = '';
			}
		}
	}
	
	##########
	### DB ###
	##########
	public function dbmsCreateDB(string $dbName): void
	{
		$this->dbmsQry("CREATE DATABASE $dbName");
	}
	
	public function dbmsUseDB(string $dbName): void
	{
		$this->dbmsQuery("USE $dbName");
	}
	
	public function dbmsDropDB(string $dbName): void
	{
		$this->dbmsQry("DROP DATABASE $dbName");
	}
	
	##############
	### Schema ###
	##############
	public function dbmsTableExists(string $tableName): bool
	{
		$query = "SELECT EXISTS (SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'music' AND TABLE_TYPE LIKE 'BASE TABLE' AND TABLE_NAME = 'Artists');";
		return !!$this->dbmsQry($query);
	}
	
	public function dbmsCreateTable(GDO $gdo): void
	{
		$query = $this->dbmsCreateTableCode($gdo);
		$this->dbmsQry($query);
	}
	
	public function dbmsCreateTableCode(GDO $gdo): string
	{
		$columns = [];
		$primary = [];
		
		foreach ($gdo->gdoColumnsCache() as $column)
		{
			if ($define = $column->gdoColumnDefine())
			{
				$columns[] = $define;
			}
			if (isset($column->primary) && $column->primary) # isPrimary() not used, because of AutoInc hack.
			{
				$primary[] = $column->identifier();
			}
		}
		
		if (count($primary))
		{
			$primary = implode(',', $primary);
			$columns[] = "PRIMARY KEY ($primary) " . Database::PRIMARY_USING;
		}
		
		foreach ($gdo->gdoColumnsCache() as $column)
		{
			if ($column->isUnique())
			{
				$columns[] = "UNIQUE({$column->identifier()})";
			}
		}
		
		$columnsCode = implode(",\n", $columns);
		
		$query = "CREATE TABLE IF NOT EXISTS {$gdo->gdoTableIdentifier()} ".
			"(\n$columnsCode\n) ENGINE = {$gdo->gdoEngine()}";
		
		return $query;
	}
	
	public function dbmsTruncateTable(string $tableName): void
	{
		$this->dbmsQry("TRUNCATE TABLE {$tableName}");
	}
	
	public function dbmsDropTable(string $tableName): void
	{
		$this->dbmsQry("DROP TABLE IF EXISTS {$tableName}");
	}
	
	#################
	### Migration ###
	#################
	/**
	 * Automigrations are pretty kewl.
	 */
	public function dbmsAutoMigrate(GDO $gdo): void
	{
		# Remove old temp table
		$tablename = $gdo->gdoTableName();
		$temptable = "zzz_temp_{$tablename}";
		$this->dbmsDropTable($temptable);
		
		# create temp and copy as old
		$this->dbmsForeignKeys(false);
		# Do not! drop the temp table. It might contain live data from a failed upgrade
		$query = "SHOW CREATE TABLE $tablename";
		$result = $this->dbmsQry($query);
		$query = mysqli_fetch_row($result)[1];
		$query = str_replace($tablename, $temptable, $query);
		$this->dbmsQry($query);
		$query = "INSERT INTO $temptable SELECT * FROM $tablename";
		$this->dbmsQry($query);
		
		# drop existing and recreate as new
		$query = "DROP TABLE $tablename";
		$this->dbmsQry($query);
		$gdo->createTable(); # CREATE TABLE IF NOT EXIST
		
		# calculate columns and copy back in new
		if ($columns = $this->columnNames($gdo, $temptable))
		{
			$columns = implode(',', $columns);
			$query = "INSERT INTO $tablename ($columns) SELECT $columns FROM $temptable";
			$this->dbmsQry($query);
			
			# drop temp after all succeded.
			$query = "DROP TABLE $temptable";
			$this->dbmsQry($query);
		}
	}
	
	private function dbmsColumnNames(GDO $gdo, string $temptable): ?array
	{
		$db = GDO_DB_NAME;
		
		$query = "SELECT group_concat(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS " .
			"WHERE TABLE_SCHEMA = '{$db}' AND TABLE_NAME = '{$temptable}'";
		$result = $this->dbmsQuery($query);
		$old = mysqli_fetch_array($result)[0];
		$old = explode(',', $old);
		
		$query = "SELECT group_concat(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS " .
			"WHERE TABLE_SCHEMA = '{$db}' AND TABLE_NAME = '{$gdo->gdoTableName()}'";
		$result = $this->dbmsQuery($query);
		$new = mysqli_fetch_array($result)[0];
		$new = explode(',', $new);
		return ($old && $new) ?
			array_intersect($old, $new) : [];
	}
	
}

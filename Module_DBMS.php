<?php
declare(strict_types=1);
namespace GDO\DBMS;

use GDO\Core\Application;
use GDO\Core\Debug;
use GDO\Core\GDO;
use GDO\Core\GDO_DBException;
use GDO\Core\GDO_Exception;
use GDO\Core\GDO_Module;
use GDO\Core\GDT;
use GDO\Core\GDT_AutoInc;
use GDO\Core\GDT_Char;
use GDO\Core\GDT_Checkbox;
use GDO\Core\GDT_CreatedAt;
use GDO\Core\GDT_DBField;
use GDO\Core\GDT_Decimal;
use GDO\Core\GDT_Enum;
use GDO\Core\GDT_Float;
use GDO\Core\GDT_Index;
use GDO\Core\GDT_Int;
use GDO\Core\GDT_Object;
use GDO\Core\GDT_ObjectSelect;
use GDO\Core\GDT_String;
use GDO\Core\GDT_Text;
use GDO\Date\GDT_Date;
use GDO\Date\GDT_DateTime;
use GDO\Date\GDT_Time;
use GDO\Date\GDT_Timestamp;
use GDO\DB\Database;
use GDO\TBS\GDO_TBS_ChallengeSolvedCategory;
use mysqli;
use mysqli_result;

/**
 * MySQLi DBMS module.
 *
 * *NEW* This is a required multi-provider module.
 * Minimalistic API to support a DBMS.
 * Configuration is done in protected/config.php
 *
 * The DBMS *only* has to generate create code for around 18 core types.
 * The rest of the system uses only these core types to generate composites or alikes.
 *
 * The-Auto-Migration is worth a look!
 *
 * @version 7.0.3
 * @since 7.0.2
 * @author gizmore
 */
final class Module_DBMS extends GDO_Module
{

	public int $priority = 7;

	private ?mysqli $link = null;

	public function __wakeup(): void
	{
		parent::__wakeup();
		$this->link = null;
	}

	##############
	### Module ###
	##############
	public function checkSystemDependencies(): bool
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

	public function dbmsClose(): void
	{
		if ($this->link)
		{
			mysqli_close($this->link);
			$this->link = null;
		}
	}

	public function dbmsFree(mysqli_result $result): void
	{
		mysqli_free_result($result);
	}

	public function dbmsFetchRow(mysqli_result $result): ?array
	{
		return mysqli_fetch_row($result);
	}

	public function dbmsFetchAllRows(mysqli_result $result): ?array
	{
		return mysqli_fetch_all($result);
	}

	public function dbmsFetchAssoc(mysqli_result $result): ?array
	{
		return mysqli_fetch_assoc($result);
	}

	public function dbmsFetchAllAssoc(mysqli_result $result): ?array
	{
		return mysqli_fetch_all($result, MYSQLI_ASSOC);
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

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsLock(string $lock, int $timeout = 30): bool
	{
		$query = "SELECT GET_LOCK('{$lock}', {$timeout}) as L";
		return !!$this->dbmsQuery($query, false);
	}

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsQuery(string $query, bool $buffered = true): mysqli_result|bool
	{
		try
		{
			return $buffered ?
				mysqli_query($this->link, $query) :
				mysqli_query($this->link, $query);
		}
		catch (\Throwable $ex)
		{
			Debug::debugException($ex, false);
			$msg = $this->dbmsError() ?: $ex->getMessage();
			throw new GDO_DBException($this->dbmsErrno(), $msg, $query, $ex);
		}
	}

	public function dbmsErrno(): int
	{
		return mysqli_errno($this->link);
	}

	public function dbmsError(): string
	{
		return mysqli_error($this->link);
	}

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsUnlock(string $lock): bool
	{
		$query = "SELECT RELEASE_LOCK('{$lock}') AS L";
		return !!$this->dbmsQuery($query, false);
	}

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsExecFile(string $path): void
	{
		$fh = fopen($path, 'r');
		$command = '';
		while ($line = fgets($fh))
		{
			$line = ltrim($line);
			if (
				(str_starts_with($line, '-- ')) ||
				(str_starts_with($line, '/*'))
			)
			{
				# skip comments
				continue;
			}

			# Append to command
			$command .= $line . "\n";

			# Finished command
			if (str_ends_with($line, ';'))
			{
				# Most likely a write
				$this->dbmsQry($command);
				$command = '';
			}
		}
	}

	/**
	 * An unbuffered write query.
	 * @throws GDO_DBException
	 */
	public function dbmsQry(string $query): mysqli_result|bool
	{
		return $this->dbmsQuery($query, false);
	}

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsCreateDB(string $dbName): void
	{
		$this->dbmsQry("CREATE DATABASE {$dbName}");
	}

	############
	### Bulk ###
	############

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsUseDB(string $dbName): void
	{
		$this->dbmsQry("USE {$dbName}");
	}

	##########
	### DB ###
	##########

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsDropDB(string $dbName): void
	{
		$this->dbmsOpen(GDO_DB_HOST, GDO_DB_USER, GDO_DB_PASS, null, GDO_DB_PORT);
		$this->dbmsQry("DROP DATABASE IF EXISTS {$dbName}");
	}

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsOpen(string $host, string $user, string $pass, string $database = null, int $port = 3306): mysqli
	{
		if (!$this->link)
		{
			$this->link = mysqli_connect($host, $user, $pass, $database, $port);
			$this->dbmsQuery('SET NAMES UTF8');
			$this->dbmsQuery("SET time_zone = '+00:00'");
			if ($database)
			{
				$this->dbmsUseDB($database);
			}
		}
		return $this->link;
	}

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsTableExists(string $tableName): bool
	{
		$dbName = Database::instance()->usedb;
		$query = "SELECT EXISTS (SELECT TABLE_NAME FROM information_schema.TABLES
                                 WHERE TABLE_SCHEMA LIKE '{$dbName}'
                                 AND TABLE_TYPE LIKE 'BASE TABLE'
                                 AND TABLE_NAME = '{$tableName}')";
		return $this->dbmsQry($query);
	}

	##############
	### Schema ###
	##############

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsCreateTable(GDO $gdo): void
	{
		$this->dbmsQry($this->dbmsCreateTableCode($gdo));
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
				# isPrimary() is *not* used because of AutoINC hacks.
				if (isset($column->primary) && $column->primary)
				{
					$primary[] = $column->getName();
				}
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
				$columns[] = "UNIQUE({$column->getName()})";
			}
		}

		$columnsCode = implode(",\n", $columns);

		return "CREATE TABLE IF NOT EXISTS {$gdo->gdoTableIdentifier()} " .
			"(\n$columnsCode\n) ENGINE = {$gdo->gdoEngine()}";
	}

	/**
	 * @throws GDO_DBException
	 */
	public function dbmsTruncateTable(string $tableName): void
	{
		$this->dbmsQry("TRUNCATE TABLE {$tableName}");
	}

	/**
	 * Create columns schema code for a single GDT.
	 * @throws GDO_DBException
	 */
	public function dbmsSchema(GDT $gdt): string
	{
		$classes = class_parents($gdt);
		array_unshift($classes, get_class($gdt));
		foreach ($classes as $classname)
		{
			$classname = substr($classname, 4);
			$classname = str_replace('\\', '_', $classname);
			if (method_exists($this, $classname))
			{
				return call_user_func([$this, $classname], $gdt);
			}
		}
		throw new GDO_DBException('err_gdt_column_define_missing', [
			$gdt->getName(), get_class($gdt)]);
	}

	public function dbmsQuote($var): string
	{
		return "'{$this->dbmsEscape($var)}'";
	}

	public function dbmsEscape(string $var): string
	{
		return str_replace(
			['\\', "'", '"'],
			['\\\\', '\\\'', '\\"'], $var);
	}

	public function dbmsEscapeSearch(string $var): string
	{
		return str_replace(
			['%', "'", '"', '\\'],
			['\\%', "\\'", '\\"', '\\\\'],
			$var);
	}


	##############
	### Compat ###
	##############

	public function dbmsRandom(): string
	{
		return 'RAND()';
	}

	public function dbmsConcat(string ...$fields): string
	{
		return sprintf('CONCAT(%s)', implode(',', $fields));
	}

	public function dbmsTimestamp(string $arg): string
	{
		return sprintf('UNIX_TIMESTAMP(%s)', $arg);
	}

	public function dbmsFromUnixtime(int $time = 0): string
	{
		$time = $time ?: Application::$TIME;
		return "FROM_UNIXTIME({$time})";
	}

	public function Core_GDT_AutoInc(GDT_AutoInc $gdt): string
	{
		return "{$gdt->name} {$this->gdoSizeDefine($gdt)}INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY";
	}

	private function gdoSizeDefine(GDT_Int $gdt): string
	{
		switch ($gdt->bytes)
		{
			case 1:
				return 'TINY';
			case 2:
				return 'MEDIUM';
			case 4:
				return '';
			case 8:
				return 'BIG';
			default:
				return "!ERR_INT_BYTES!={$gdt->bytes}";
		}
	}

	###############
	### Columns ###
	###############

	public function Core_GDT_Int(GDT_Int $gdt): string
	{
		$unsigned = $gdt->unsigned ? ' UNSIGNED' : '';
		return "{$gdt->name} {$this->gdoSizeDefine($gdt)}INT{$unsigned}{$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	private function gdoNullDefine(GDT_DBField $gdt): string
	{
		return $gdt->notNull ? ' NOT NULL' : ' NULL';
	}

	private function gdoInitialDefine(GDT_DBField $gdt): string
	{
		return $gdt->initial ? (' DEFAULT ' . GDO::quoteS($gdt->initial)) : GDT::EMPTY_STRING;
	}

	public function Core_GDT_Enum(GDT_Enum $gdt): string
	{
		$values = implode(',', array_map([GDO::class, 'quoteS'], $gdt->enumValues));
		return "{$gdt->name} ENUM ($values) CHARSET ascii COLLATE ascii_bin {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_Checkbox(GDT_Checkbox $gdt): string
	{
		return "{$gdt->name} TINYINT(1) UNSIGNED {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_Float(GDT_Float $gdt): string
	{
		$unsigned = $gdt->unsigned ? ' UNSIGNED' : GDT::EMPTY_STRING;
		return "{$gdt->name} FLOAT{$unsigned}{$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_Decimal(GDT_Decimal $gdt): string
	{
		$digits = sprintf('%d,%d', $gdt->digitsBefore + $gdt->digitsAfter, $gdt->digitsAfter);
		return "{$gdt->name} DECIMAL($digits){$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_Char(GDT_Char $gdt): string
	{
		$collate = $this->gdoCollateDefine($gdt, $gdt->caseSensitive);
		return "{$gdt->name} CHAR({$gdt->max}) CHARSET {$this->gdoCharsetDefine($gdt)} {$collate}" .
			$this->gdoNullDefine($gdt) .
			$this->gdoInitialDefine($gdt);
	}

	private function gdoCollateDefine(GDT_String $gdt, bool $caseSensitive): string
	{
		if (!$gdt->isBinary())
		{
			$append = $caseSensitive ? '_bin' : '_general_ci';
			return ' COLLATE ' . $this->gdoCharsetDefine($gdt) . $append;
		}
		return GDT::EMPTY_STRING;
	}

	private function gdoCharsetDefine(GDT_String $gdt): string
	{
		switch ($gdt->encoding)
		{
			case GDT_String::UTF8:
				return 'utf8mb4';
			case GDT_String::ASCII:
				return 'ascii';
			case GDT_String::BINARY:
				return 'binary';
			default:
				return '!INVALID!CHARSET!';
		}
	}

	public function Core_GDT_String(GDT_String $gdt): string
	{
		$charset = $this->gdoCharsetDefine($gdt);
		$collate = $this->gdoCollateDefine($gdt, $gdt->caseSensitive);
		$null = $this->gdoNullDefine($gdt);
		return "{$gdt->name} VARCHAR({$gdt->max}) CHARSET {$charset}{$collate}{$null}";
	}

	public function Core_GDT_Text(GDT_Text $gdt): string
	{
		return $gdt->name . ' ' . $this->Core_GDT_TextB($gdt);
	}

	public function Core_GDT_TextB(GDT_Text $gdt): string
	{
		$collate = $this->gdoCollateDefine($gdt, $gdt->caseSensitive);
		return "TEXT({$gdt->max}) CHARSET {$this->gdoCharsetDefine($gdt)}{$collate}{$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Core_GDT_CreatedAt(GDT_CreatedAt $gdt): string
	{
		return "{$gdt->name} TIMESTAMP({$gdt->millis}){$this->gdoNullDefine($gdt)} DEFAULT CURRENT_TIMESTAMP({$gdt->millis})";
	}

	public function Date_GDT_Date(GDT_Date $gdt): string
	{
		return "{$gdt->name} DATE {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Date_GDT_DateTime(GDT_DateTime $gdt): string
	{
		return "{$gdt->name} DATETIME({$gdt->millis}) {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Date_GDT_Time(GDT_Time $gdt): string
	{
		return "{$gdt->name} TIME {$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	public function Date_GDT_Timestamp(GDT_Timestamp $gdt): string
	{
		return "{$gdt->name} TIMESTAMP({$gdt->millis}){$this->gdoNullDefine($gdt)}{$this->gdoInitialDefine($gdt)}";
	}

	##############
	### Helper ###
	##############

	/**
	 * @throws GDO_DBException
	 */
	public function Core_GDT_ObjectSelect(GDT_ObjectSelect $gdt): string
	{
		return $this->Core_GDT_Object($gdt);
	}

	/**
	 * Take the foreign key primary key definition and use str_replace to convert to foreign key definition.
	 *
	 * @throws GDO_Exception
	 */
	public function Core_GDT_Object(GDT_Object|GDT_ObjectSelect $gdt): string
	{
		if (!($table = $gdt->table))
		{
			throw new GDO_Exception('err_gdo_object_no_table', [
				$gdt->getName(),
			], GDO_Exception::DB_ERROR_CODE);
		}
		$tableName = $table->gdoTableIdentifier();
		if (!($primaryKey = $table->gdoPrimaryKeyColumn()))
		{
			throw new GDO_Exception('err_gdo_no_primary_key', [
				$tableName,
				$gdt->getName(),
			], GDO_Exception::DB_ERROR_CODE);
		}
		$define = $primaryKey->gdoColumnDefine();
		$define = str_replace($primaryKey->getName(), $gdt->getName(), $define);
		$define = str_replace(' NOT NULL', '', $define);
		$define = str_replace(' PRIMARY KEY', '', $define);
		$define = str_replace(' AUTO_INCREMENT', '', $define);
		$define = preg_replace('#,FOREIGN KEY .* ON UPDATE (?:CASCADE|RESTRICT|SET NULL)#', '', $define);
		$on = $primaryKey->getName();
		return "$define{$this->gdoNullDefine($gdt)}" .
			",FOREIGN KEY ({$gdt->name}) REFERENCES $tableName($on) ON DELETE {$gdt->cascade} ON UPDATE CASCADE";
	}

	public function Core_GDT_Index(GDT_Index $gdt): string
	{
		return "{$this->gdoFulltextDefine($gdt)} INDEX({$gdt->indexColumns}) {$this->gdoIndexDefine($gdt)}";
	}

	public function dbmsDropTable(string $tableName): bool
	{
		try
		{
			return $this->dbmsQry("DROP TABLE IF EXISTS {$tableName}");
		}
		catch (GDO_DBException $ex)
		{
			Debug::debugException($ex);
			return false;
		}
	}

	public function dbmsForeignKeys(bool $foreignKeysEnabled): bool
	{
		try
		{
			$check = (int)$foreignKeysEnabled;
			return $this->dbmsQry("SET foreign_key_checks = {$check}");
		}
		catch (GDO_DBException $ex)
		{
			Debug::debugException($ex);
			return false;
		}
	}

	private function gdoFulltextDefine(GDT_Index $gdt): string
	{
		return $gdt->indexFulltext ?? GDT::EMPTY_STRING;
	}

	private function gdoIndexDefine(GDT_Index $gdt): string
	{
		return $gdt->indexUsing ?? GDT::EMPTY_STRING;
	}


	#################
	### Migration ###
	#################
	/**
	 * Automigrations are pretty kewl.
	 *
	 * @throws GDO_DBException
	 */
	public function dbmsAutoMigrate(GDO $gdo)
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
		$gdo->createTable();

		# calculate columns and copy back in new
		if ($columns = $this->dbmsColumnNames($gdo, $temptable))
		{
			$columns = implode(',', $columns);
			$query = "INSERT INTO $tablename ($columns) SELECT $columns FROM $temptable";
			$this->dbmsQry($query);

			# drop temp after all succeded.
			$query = "DROP TABLE $temptable";
			$this->dbmsQry($query);
		}
	}


	/**
	 * Get the table columns that are both intersecting to migrate the old data.
	 * @throws GDO_DBException
	 */
	private function dbmsColumnNames(GDO $gdo, string $temptable): array
	{
		$db = GDO_DB_NAME;

		# Old column names
		$query = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ' .
			"WHERE TABLE_SCHEMA = '{$db}' AND TABLE_NAME = '{$temptable}'";
		$result = $this->dbmsQuery($query);
		$rows = mysqli_fetch_all($result);
		$old = array_map(function (array $row)
		{
			return $row[0];
		}, $rows);

		# New column names
		$query = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ' .
			"WHERE TABLE_SCHEMA = '{$db}' AND TABLE_NAME = '{$gdo->gdoTableName()}'";
		$result = $this->dbmsQuery($query);
		$rows = mysqli_fetch_all($result);
		$new = array_map(function (array $row)
		{
			return $row[0];
		}, $rows);
		return ($old && $new) ? array_intersect($old, $new) : GDT::EMPTY_ARRAY;
	}

}

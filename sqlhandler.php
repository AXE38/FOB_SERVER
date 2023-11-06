<?php
	Class sqlhandler {
		private $sName;
		private $sConn;
		private $conn;

		function __construct() {
       		$xml = simplexml_load_file('config.xml');
			$this->sName = (string)$xml->ServerName;
			$DB = (string)$xml->Database;
			$UID = (string)$xml->UID;
			$PWD = (string)$xml->PWD;

			$this->sConn = "host={$this->sName} port=5432 dbname={$DB} user={$UID} password={$PWD} application_name=abs.backend";
			$this->conn = @pg_connect($this->sConn);
   		}

		function sql_exec($sql, $params = array()) {
			$stmt = @pg_query($this->conn, $sql);
			if($stmt === false ) {
			    if (pg_last_error() != null) {
			            return pg_last_error();
			    }
			}
			$result = array();
			while( $row = pg_fetch_array( $stmt, null, PGSQL_ASSOC) ) {
				array_push($result, $row);	
			}
			return $result;	
		}
	}
?>
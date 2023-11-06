<?php
	include 'sqlhandler.php';
	error_reporting(E_ALL & ~E_NOTICE);
	$sql = new SqlHandler();
	$user_id = 0;

	function getError($errorcode) {
		$xml = new simplexmlElement('<root/>');
		$node1 = $xml->addChild('state', $errorcode);
		return $xml->asXML();
	}

	function getColumns($type, $entity) {
		$columns = "";
		$values = "";
		foreach ($entity->children() as $key => $val) {
			switch (trim($type)) {
				case "INSERT":
					$columns = $columns . $key . ",";
					$values = $values . "'" . $val . "',";
					break;
				case "UPDATE":
					$values = $values . $key . "='" . $val . "',";
					break;
				case "SELECT":
					$columns = $columns . $key . ",";
					break;
			}
		}
		$columns = rtrim($columns, ",");
		$values = rtrim($values, ",");
		return array("columns"=>$columns, "values"=>$values);
	}

	function getWhere($where) {
		if ( !((bool)$where) || count($where->children()) == 0) {
			return " ";
		}
		$expr = " where ";
		foreach ($where->condition as $val) {
			if (trim($val->column) == "abs_password") {
				$val->value = hash("sha256", $val->value);
			}
			$expr = $expr . $val->column . " " . $val->operator . " '" . $val->value . "' AND ";
		}
		$expr = rtrim($expr, " AND ");
		return $expr;
	}

	function Auth($where) {
		global $sql;

		if (!((bool)$where)) {
			echo getError(-3);
			exit;
		}

		$operator = "select id, token, role_id from ABS_USER " . getWhere($where);
		$res = $sql->sql_exec($operator);

		if (count($res) == 0) {
			echo getError(-3);
			exit;
		} else {
			$res = $res[0];
			$id = $res['id'];
			$token = $res['token'];
			$role_id = $res['role_id'];
			if ($token == "") {
				$token = hash("sha256", date(DATE_ATOM));
			}
			$dt = date('Y-m-d H:i:s');
			$operator = "update ABS_USER set last_login_date = '" . $dt . "', token = '" . $token . "' where id = " . $id;
			$res = $sql->sql_exec($operator);
			$xml = new simplexmlElement('<root/>');
			$node1 = $xml->addChild('state', '200');
			$node1 = $xml->addChild('token', $token);
			$node1 = $xml->addChild('role_id', $role_id);
			echo $xml->asXML();
		}
	}

	function CheckAuth($token) {
		global $sql;

		$operator = "select id, token from ABS_USER where token = '{$token}'";
		$res = $sql->sql_exec($operator);
		return $res;
		if (count($res) == 0) {
			echo getError(-3);
			exit;
		}
	}

	function getSql($type, $entity_type, $entity, $token, $where = "", $offset = "", $fetch = "") {
		switch (trim($type)) {
			case "INSERT":
				$arr = getColumns($type, $entity);
				$operator = "insert into " . $entity_type . " (" . $arr['columns'] . ") values (" . $arr['values'] . ");";
				break;
			case "UPDATE":
				$operator = "SET LOCAL abs_cfg.abs_cfg.update_before_delete = '';";
				$arr = getColumns($type, $entity);
				$operator .= "update " . $entity_type . " set " . $arr['values'] . " " . getWhere($where);
				break;
			case "SELECT":
				$arr = getColumns($type, $entity);
				$operator = "select " . $arr['columns'] . " from " . $entity_type . " " . getWhere($where);
				if ($offset != "" && $fetch != "") {
					$operator = $operator . " order by 1 offset {$offset} rows fetch next {$fetch} rows only";
				};
				break;
			case "DELETE":
				$dt = date('Y-m-d H:i:s');
				$operator = "select set_config('abs_cfg.abs_cfg.update_before_delete', '1', false);";
				$operator .= "update " . $entity_type . " set last_update_date = " . $dt . " " . getWhere($where);
				$operator .= "delete from " . $entity_type . " " . getWhere($where);
				break;
			break;
		}
		$stmt = $operator . " ";
		return $stmt;
	}

	function getXmlFromRes($res) {
		
		$xml = new simplexmlElement('<root/>');
		$node1 = $xml->addChild('state', '200');
		$node1 = $xml->addChild('data');
		foreach ($res as $row) {
			$node2 = $node1->addChild('row');
			foreach ($row as $key=>$val) {
				if ($val instanceof DateTime) {
					$val = $val->format('Y-m-d H:i:s');
				}
				$node3 = $node2->addChild($key, $val);
			}
		}
		echo $xml->asXML();
	}

	libxml_clear_errors();
	libxml_use_internal_errors(TRUE);
	if (!isset($_POST['request']) and !isset($_GET['test'])) {
		echo getError(-1);
		exit;
	} elseif (isset($_POST['request'])) {
		$request = $_POST['request'];
	}
	
	$type = null;
	try {
		$xml = new simplexmlElement($request, LIBXML_NOWARNING | LIBXML_NOERROR);
		$type = $xml->type;
		$entity = $xml->entity;
		$entity_type = $xml->entity_type;
		$token = $xml->token;
		$where = $xml->where;
		$offset = $xml->offset;
		$fetch = $xml->fetch;
	} catch (Exception $e) {
		echo getError(-2);
		exit;
	} finally {
		libxml_clear_errors();
	}
	if (trim($type) == "AUTH") {
		Auth($where);
		exit;
	} else {
		$res = CheckAuth(trim($token));
		$user_id = $res['id'];
	}
	$stmt = getSql($type, $entity_type, $entity, $token, $where, $offset, $fetch);
	$res = $sql->sql_exec($stmt);

	if (gettype($res) === "string") {
		echo getError(-5);
		echo $res;
		exit;
	}
	echo getXmlFromRes($res);
?>

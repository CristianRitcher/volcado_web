<?php
header('Content-Type:application/json');
$_method = $_SERVER['REQUEST_METHOD'];
$data = json_decode(file_get_contents('php://input'), true);
$base = '/volcado_web';

switch ($_method) {
    case 'POST':
        if (!isset($data['modo'])) {
            http_response_code(400);
            echo json_encode([
                "status" => "error",
                "message" => 'El campo "modo" es requerido. Use "apertura" o "cierre"'
            ]);
            exit;
        }
        //modo
        $modo = $data['modo'];

        //indo del origen
        $db_origen = $data['db_origen'];
        $db_origen_password = $data['db_password_origen'];
        
        $db_origen_alias = $db_origen['alias'];
        $db_origen_host = $db_origen['host'];
        $db_origen_user = $db_origen['user'];
        $db_origen_database = $db_origen['database'];

        //indio del destino
        $db_destino = $data['db_destino'];
        $db_destino_password = $data['db_password_destino'];

        $db_destino_host = $db_destino['host'];
        $db_destino_user = $db_destino['user'];
        $db_destino_database = $db_destino['database'];


        //argumentos del modo
        $modo_arg = escapeshellarg($modo);

        //argumentos del origen
        $db_origen_password_arg = escapeshellarg($db_origen_password);
        
        $db_origen_alias_arg = escapeshellarg($db_origen_alias);
        $db_origen_host_arg = escapeshellarg($db_origen_host);
        $db_origen_user_arg = escapeshellarg($db_origen_user);
        $db_origen_database_arg = escapeshellarg($db_origen_database);

        //argumentos del destino
        $db_destino_password_arg = escapeshellarg($db_destino_password);
        
        $db_destino_host_arg = escapeshellarg($db_destino_host);
        $db_destino_user_arg = escapeshellarg($db_destino_user);
        $db_destino_database_arg = escapeshellarg($db_destino_database);

        // Ejecutar con argumentos
        $cmd = "python3 sync.py $db_destino_host_arg:$db_destino_user_arg:$db_destino_password_arg:$db_destino_database_arg --sources $db_origen_alias_arg=$db_origen_host_arg:$db_origen_user_arg:$db_origen_password_arg:$db_origen_database_arg --modo $modo_arg";
        $return_var = 0;
        exec($cmd . " 2>&1", $output, $return_var);

        if ($return_var !== 0) {
            http_response_code(500);
            echo json_encode([
                "status" => "error",
                "message" => implode("\n", $output)
            ]);
            exit;
        }

        // La salida de Python ya es JSON
        echo implode("\n", $output);
        exit;

        break;
}

-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Servidor: 127.0.0.1
-- Tiempo de generación: 14-09-2025 a las 23:19:33
-- Versión del servidor: 10.4.32-MariaDB
-- Versión de PHP: 8.2.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Base de datos: `restaurante`
--
CREATE DATABASE IF NOT EXISTS `restaurante` DEFAULT CHARACTER SET utf16le COLLATE utf16le_bin;
USE `restaurante`;

DELIMITER $$
--
-- Procedimientos
--
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_archivar_transaccion` (IN `p_ticket_id` INT)   BEGIN
  DECLARE v_dst VARCHAR(64) DEFAULT 'restaurante_espejo';
  DECLARE v_venta_id INT;
  DECLARE v_tiene_factura INT DEFAULT 0;
  DECLARE v_src_has_ticket_propinas INT DEFAULT 0;

  /* Resolver venta del ticket */
  SELECT venta_id INTO v_venta_id
  FROM restaurante.tickets
  WHERE id = p_ticket_id
  LIMIT 1;

  IF v_venta_id IS NULL THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Ticket inexistente o sin venta asociada';
  END IF;

  /* Bloquear si el ticket está facturado (no cancelada) */
  SELECT COUNT(*) INTO v_tiene_factura
  FROM restaurante.facturas
  WHERE ticket_id = p_ticket_id
    AND COALESCE(estado,'generada') <> 'cancelada';

  IF v_tiene_factura > 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No se puede archivar: el ticket tiene factura';
  END IF;

  /* Evitar efectos secundarios de triggers durante el borrado */
  SET @ARCHIVE_MODE = 1;

  START TRANSACTION;

  /* ======================= PREP ======================= */
  DROP TEMPORARY TABLE IF EXISTS tmp_detalles;
  CREATE TEMPORARY TABLE tmp_detalles (PRIMARY KEY(id))
  AS SELECT id FROM restaurante.venta_detalles WHERE venta_id = v_venta_id;

  DROP TEMPORARY TABLE IF EXISTS tmp_tickets;
  CREATE TEMPORARY TABLE tmp_tickets (PRIMARY KEY(id))
  AS SELECT id FROM restaurante.tickets WHERE venta_id = v_venta_id;

  /* ======================= COPIAS AL ESPEJO ======================= */

  -- ventas (arrastra corte_id)
  SET @sql = CONCAT('INSERT IGNORE INTO ', v_dst, '.ventas SELECT * FROM restaurante.ventas WHERE id=?');
  PREPARE s1 FROM @sql; SET @p := v_venta_id; EXECUTE s1 USING @p; DEALLOCATE PREPARE s1;

  -- venta_detalles (omitir subtotal generado)
  SET @sql = CONCAT(
    'INSERT IGNORE INTO ', v_dst, '.venta_detalles ',
    '(id, venta_id, producto_id, cantidad, precio_unitario, insumos_descargados, created_at, entregado_hr, estado_producto, observaciones) ',
    'SELECT id, venta_id, producto_id, cantidad, precio_unitario, insumos_descargados, created_at, entregado_hr, estado_producto, observaciones ',
    'FROM restaurante.venta_detalles WHERE venta_id=?'
  );
  PREPARE s2 FROM @sql; SET @p := v_venta_id; EXECUTE s2 USING @p; DEALLOCATE PREPARE s2;

  -- venta_detalles_log
  SET @sql = CONCAT(
    'INSERT IGNORE INTO ', v_dst, '.venta_detalles_log ',
    'SELECT l.* FROM restaurante.venta_detalles_log l ',
    'JOIN tmp_detalles d ON d.id = l.venta_detalle_id'
  );
  PREPARE s3 FROM @sql; EXECUTE s3; DEALLOCATE PREPARE s3;

  -- venta_detalles_cancelados
  SET @sql = CONCAT(
    'INSERT IGNORE INTO ', v_dst, '.venta_detalles_cancelados ',
    'SELECT * FROM restaurante.venta_detalles_cancelados WHERE venta_id=?'
  );
  PREPARE s4 FROM @sql; SET @p := v_venta_id; EXECUTE s4 USING @p; DEALLOCATE PREPARE s4;

  -- tickets (arrastra campos extra si existen, p.ej. corte_id)
  SET @sql = CONCAT('INSERT IGNORE INTO ', v_dst, '.tickets ',
                    'SELECT * FROM restaurante.tickets WHERE venta_id=?');
  PREPARE s5 FROM @sql; SET @p := v_venta_id; EXECUTE s5 USING @p; DEALLOCATE PREPARE s5;

  -- ticket_detalles (omitir subtotal generado)
  SET @sql = CONCAT(
    'INSERT IGNORE INTO ', v_dst, '.ticket_detalles ',
    '(id, ticket_id, producto_id, cantidad, precio_unitario) ',
    'SELECT id, ticket_id, producto_id, cantidad, precio_unitario ',
    'FROM restaurante.ticket_detalles ',
    'WHERE ticket_id IN (SELECT id FROM tmp_tickets)'
  );
  PREPARE s6 FROM @sql; EXECUTE s6; DEALLOCATE PREPARE s6;

  -- ticket_descuentos
  SET @sql = CONCAT(
    'INSERT IGNORE INTO ', v_dst, '.ticket_descuentos ',
    '(id, ticket_id, tipo, venta_detalle_id, porcentaje, monto, motivo, usuario_id, catalogo_promo_id, creado_en) ',
    'SELECT id, ticket_id, tipo, venta_detalle_id, porcentaje, monto, motivo, usuario_id, catalogo_promo_id, creado_en ',
    'FROM restaurante.ticket_descuentos ',
    'WHERE ticket_id IN (SELECT id FROM tmp_tickets)'
  );
  PREPARE s6b FROM @sql; EXECUTE s6b; DEALLOCATE PREPARE s6b;

  -- ticket_propinas (si existe en origen y en espejo)
  SELECT COUNT(*) INTO v_src_has_ticket_propinas
  FROM information_schema.tables
  WHERE table_schema = 'restaurante' AND table_name = 'ticket_propinas';

  IF v_src_has_ticket_propinas > 0 THEN
    BEGIN
      DECLARE CONTINUE HANDLER FOR 1146 BEGIN END;  -- espejo no tiene la tabla
      SET @sql = CONCAT(
        'INSERT IGNORE INTO ', v_dst, '.ticket_propinas ',
        'SELECT * FROM restaurante.ticket_propinas ',
        'WHERE ticket_id IN (SELECT id FROM tmp_tickets)'
      );
      PREPARE s6c FROM @sql; EXECUTE s6c; DEALLOCATE PREPARE s6c;
    END;
  END IF;

  -- log_cancelaciones
  SET @sql = CONCAT(
    'INSERT IGNORE INTO ', v_dst, '.log_cancelaciones ',
    'SELECT lc.* FROM restaurante.log_cancelaciones lc ',
    'LEFT JOIN tmp_detalles d ON lc.venta_detalle_id = d.id ',
    'WHERE lc.venta_id = ? OR d.id IS NOT NULL'
  );
  PREPARE s7 FROM @sql; SET @p := v_venta_id; EXECUTE s7 USING @p; DEALLOCATE PREPARE s7;

  /* ======================= BORRADO EN OPERATIVO ======================= */

  DELETE FROM restaurante.ticket_descuentos
  WHERE ticket_id IN (SELECT id FROM tmp_tickets);

  IF v_src_has_ticket_propinas > 0 THEN
    DELETE FROM restaurante.ticket_propinas
    WHERE ticket_id IN (SELECT id FROM tmp_tickets);
  END IF;

  DELETE FROM restaurante.ticket_detalles
  WHERE ticket_id IN (SELECT id FROM tmp_tickets);

  DELETE FROM restaurante.tickets
  WHERE venta_id = v_venta_id;

  DELETE FROM restaurante.log_cancelaciones
  WHERE venta_id = v_venta_id
     OR venta_detalle_id IN (SELECT id FROM tmp_detalles);

  DELETE FROM restaurante.venta_detalles_log
  WHERE venta_detalle_id IN (SELECT id FROM tmp_detalles);

  DELETE FROM restaurante.venta_detalles_cancelados
  WHERE venta_id = v_venta_id;

  DELETE FROM restaurante.venta_detalles
  WHERE venta_id = v_venta_id;

  DELETE FROM restaurante.ventas
  WHERE id = v_venta_id;

  COMMIT;

  SET @ARCHIVE_MODE = NULL;
END$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_cerrar_corte` (IN `p_usuario_id` INT)   BEGIN
    DECLARE v_corte_id INT;
    DECLARE total_ventas DECIMAL(10,2);

    SELECT id INTO v_corte_id 
    FROM corte_caja 
    WHERE usuario_id = p_usuario_id AND fecha_fin IS NULL 
    LIMIT 1;

    SELECT SUM(total) INTO total_ventas 
    FROM ventas
    WHERE usuario_id = p_usuario_id AND estatus = 'cerrada' AND corte_id IS NULL;


    UPDATE corte_caja
    SET fecha_fin = NOW(), total = total_ventas
    WHERE id = v_corte_id;

    UPDATE ventas
    SET corte_id = v_corte_id
    WHERE usuario_id = p_usuario_id 
      AND estatus = 'cerrada'
      AND corte_id IS NULL;
END$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_recalcular_productos_por_insumo` (IN `p_insumo_id` INT)   BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE v_producto_id INT;
    DECLARE cur CURSOR FOR
        SELECT DISTINCT r.producto_id
        FROM recetas r
        WHERE r.insumo_id = p_insumo_id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_producto_id;
        IF done THEN
            LEAVE read_loop;
        END IF;

        UPDATE productos p
        SET existencia = (
            SELECT IFNULL(MIN(FLOOR(i.existencia / r.cantidad)), 0)
            FROM recetas r
            JOIN insumos i ON r.insumo_id = i.id
            WHERE r.producto_id = v_producto_id
        )
        WHERE p.id = v_producto_id;

    END LOOP;
    CLOSE cur;
END$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_recalcular_todos` ()   BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE v_insumo_id INT;
    DECLARE cur CURSOR FOR
        SELECT DISTINCT r.insumo_id
        FROM recetas r;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    rec_loop: LOOP
        FETCH cur INTO v_insumo_id;
        IF done THEN LEAVE rec_loop; END IF;
        CALL sp_recalcular_productos_por_insumo(v_insumo_id);
    END LOOP;
    CLOSE cur;
END$$

DELIMITER ;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `alineacion`
--

CREATE TABLE `alineacion` (
  `id` int(11) NOT NULL,
  `nombre` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `alineacion`
--

INSERT INTO `alineacion` (`id`, `nombre`) VALUES
(3, 'Ala Derecha'),
(4, 'Cocina');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `catalogo_areas`
--

CREATE TABLE `catalogo_areas` (
  `id` int(11) NOT NULL,
  `nombre` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `catalogo_areas`
--

INSERT INTO `catalogo_areas` (`id`, `nombre`) VALUES
(1, 'Ala izquierda'),
(2, 'Ala derecha');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `catalogo_bancos`
--

CREATE TABLE `catalogo_bancos` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `catalogo_bancos`
--

INSERT INTO `catalogo_bancos` (`id`, `nombre`) VALUES
(1, 'BBVA'),
(2, 'Santander'),
(3, 'Banorte'),
(4, 'HSBC'),
(5, 'Scotiabank'),
(6, 'Banco Azteca'),
(7, 'Inbursa'),
(8, 'BanCoppel');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `catalogo_categorias`
--

CREATE TABLE `catalogo_categorias` (
  `id` int(11) NOT NULL,
  `nombre` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `catalogo_categorias`
--

INSERT INTO `catalogo_categorias` (`id`, `nombre`) VALUES
(1, 'Bebida'),
(2, 'Postre'),
(3, 'Platillo'),
(4, 'Sopa'),
(5, 'Arroz'),
(6, 'Extra'),
(7, 'Topping'),
(8, 'Rollo natural'),
(9, 'Rollo empanizado'),
(10, 'Entrada'),
(11, 'Rollo Premium'),
(12, 'Rollo Nano'),
(13, 'rollo horneado');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `catalogo_denominaciones`
--

CREATE TABLE `catalogo_denominaciones` (
  `id` int(11) NOT NULL,
  `valor` decimal(10,2) NOT NULL,
  `descripcion` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `catalogo_denominaciones`
--

INSERT INTO `catalogo_denominaciones` (`id`, `valor`, `descripcion`) VALUES
(1, 0.50, 'Moneda 50 centavos'),
(2, 1.00, 'Moneda 1 peso'),
(3, 2.00, 'Moneda 2 pesos'),
(4, 5.00, 'Moneda 5 pesos'),
(5, 10.00, 'Moneda 10 pesos'),
(6, 20.00, 'Moneda 20 pesos'),
(7, 50.00, 'Billete 50 pesos'),
(8, 100.00, 'Billete 100 pesos'),
(9, 200.00, 'Billete 200 pesos'),
(10, 500.00, 'Billete 500 pesos'),
(11, 1000.00, 'Billete 1000 pesos'),
(12, 1.00, 'Pago Boucher'),
(13, 1.00, 'Pago Cheque');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `catalogo_folios`
--

CREATE TABLE `catalogo_folios` (
  `id` int(11) NOT NULL,
  `descripcion` varchar(100) DEFAULT NULL,
  `folio_actual` int(11) DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `catalogo_folios`
--

INSERT INTO `catalogo_folios` (`id`, `descripcion`, `folio_actual`) VALUES
(1, 'Serie Restaurante', 1000),
(2, 'Serie Domicilio', 2068);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `catalogo_promos`
--

CREATE TABLE `catalogo_promos` (
  `id` int(11) NOT NULL,
  `nombre` varchar(150) NOT NULL,
  `descripcion` varchar(500) NOT NULL,
  `monto` decimal(10,2) NOT NULL,
  `activo` tinyint(1) NOT NULL DEFAULT 1,
  `visible_en_ticket` tinyint(1) NOT NULL DEFAULT 1,
  `tipo` enum('monto_fijo','porcentaje','bogo','combo','categoria_gratis') NOT NULL DEFAULT 'monto_fijo',
  `regla` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`regla`)),
  `tipo_venta` varchar(100) DEFAULT '''mesa''',
  `creado_en` timestamp NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `catalogo_promos`
--

INSERT INTO `catalogo_promos` (`id`, `nombre`, `descripcion`, `monto`, `activo`, `visible_en_ticket`, `tipo`, `regla`, `tipo_venta`, `creado_en`) VALUES
(1, 'TÉ GRATIS', 'promo lunes Té de Jazmín (Litro) gratis', 0.00, 1, 1, 'categoria_gratis', '{\"id_producto\": 60,\"categoria_id\":1}', 'mesa', '2025-09-08 04:29:14'),
(2, 'ENTRADA GRATIS', 'promo miércoles Entrada gratis', 0.00, 1, 1, 'categoria_gratis', '[{\"id_producto\": 35,\"categoria_id\":10},{\"id_producto\": 36,\"categoria_id\":10}]', 'mesa', '2025-09-08 04:34:53'),
(3, '2x1 ROLLOS HORNEADOS', 'promo martes 2 rollos por el precio de 1 ', 0.00, 1, 1, 'bogo', '{\"cantidad\": 2,\"categoria_id\":13}', 'mesa', '2025-09-08 04:39:56'),
(4, '3x2 ROLLOS EMPANIZADOS', 'promo jueves 3 rollos empanizados por el precio de 2 ', 0.00, 1, 1, 'bogo', '{\"cantidad\": 3,\"categoria_id\":9}', 'mesa', '2025-09-08 04:41:01');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `catalogo_tarjetas`
--

CREATE TABLE `catalogo_tarjetas` (
  `id` int(11) NOT NULL,
  `nombre` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `catalogo_tarjetas`
--

INSERT INTO `catalogo_tarjetas` (`id`, `nombre`) VALUES
(1, 'Visa'),
(2, 'MasterCard');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `clientes_facturacion`
--

CREATE TABLE `clientes_facturacion` (
  `id` int(11) NOT NULL,
  `rfc` varchar(20) NOT NULL,
  `razon_social` varchar(200) NOT NULL,
  `correo` varchar(150) DEFAULT NULL,
  `telefono` varchar(30) DEFAULT NULL,
  `calle` varchar(150) DEFAULT NULL,
  `numero_ext` varchar(20) DEFAULT NULL,
  `numero_int` varchar(20) DEFAULT NULL,
  `colonia` varchar(120) DEFAULT NULL,
  `municipio` varchar(120) DEFAULT NULL,
  `estado` varchar(120) DEFAULT NULL,
  `pais` varchar(100) DEFAULT 'México',
  `cp` varchar(10) DEFAULT NULL,
  `regimen` varchar(100) DEFAULT NULL,
  `uso_cfdi` varchar(10) DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `clientes_facturacion`
--

INSERT INTO `clientes_facturacion` (`id`, `rfc`, `razon_social`, `correo`, `telefono`, `calle`, `numero_ext`, `numero_int`, `colonia`, `municipio`, `estado`, `pais`, `cp`, `regimen`, `uso_cfdi`, `created_at`, `updated_at`) VALUES
(1, 'marf9401109i5', 'fued majul', 'fu@co.com', '6183021446', 'prueba', '45', '45', 'prueba', '', 'durango', 'México', '34000', 'persona moral', '', '2025-08-28 19:04:50', '2025-09-06 10:16:19');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `conekta_events`
--

CREATE TABLE `conekta_events` (
  `id` bigint(20) NOT NULL,
  `reference` varchar(64) DEFAULT NULL,
  `event_type` varchar(80) NOT NULL,
  `conekta_event_id` varchar(64) DEFAULT NULL,
  `payload` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`payload`)),
  `received_at` datetime NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `conekta_events`
--

INSERT INTO `conekta_events` (`id`, `reference`, `event_type`, `conekta_event_id`, `payload`, `received_at`) VALUES
(49, NULL, 'webhook_ping', '68c61ccbe942880017963f7c', '{\"data\":{\"livemode\":false,\"action\":\"webhook_ping\"},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDEfhVfknXHnKQv\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c61ccbe942880017963f7c\",\"object\":\"event\",\"type\":\"webhook_ping\",\"created_at\":1757813963}', '2025-09-13 19:39:22'),
(50, NULL, 'webhook.created', '68c61ccbe942880017963f79', '{\"data\":{\"object\":{\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"status\":\"being_pinged\",\"subscribed_events\":[\"charge.created\",\"charge.paid\",\"charge.under_fraud_review\",\"charge.fraudulent\",\"charge.refunded\",\"charge.preauthorized\",\"charge.declined\",\"charge.canceled\",\"charge.reversed\",\"charge.pending_confirmation\",\"charge.expired\",\"charge.chargeback.created\",\"charge.chargeback.updated\",\"charge.chargeback.under_review\",\"charge.chargeback.lost\",\"charge.chargeback.won\",\"charge.score_updated\",\"customer.created\",\"customer.updated\",\"customer.deleted\",\"customer.payment_source.card.blocked\",\"webhook.created\",\"webhook.updated\",\"webhook.deleted\",\"webhook_ping\",\"payout.created\",\"payout.retrying\",\"payout.paid_out\",\"payout.failed\",\"payout.in_transit\",\"plan.created\",\"plan.updated\",\"plan.deleted\",\"subscription.created\",\"subscription.paused\",\"subscription.resumed\",\"subscription.canceled\",\"subscription.expired\",\"subscription.updated\",\"subscription.paid\",\"subscription.payment_failed\",\"subscription.scheduled_payment_failed\",\"payee.created\",\"payee.updated\",\"payee.deleted\",\"payee.payout_method.created\",\"payee.payout_method.updated\",\"payee.payout_method.deleted\",\"receipt.created\",\"order.canceled\",\"order.charged_back\",\"order.created\",\"order.expired\",\"order.fraudulent\",\"order.under_fraud_review\",\"order.paid\",\"order.partially_refunded\",\"order.pending_payment\",\"order.pre_authorized\",\"order.refunded\",\"order.updated\",\"order.voided\",\"order.declined\",\"cashout.canceled\",\"cashout.confirmed\",\"cashout.expired\",\"cash_refund.created\",\"cash_refund.canceled\",\"cash_refund.refunded\",\"cash_refund.expired\",\"company.onboarding.success\",\"company.onboarding.failed\",\"inbound_payment.lookup\",\"inbound_payment.payment_attempt\",\"inbound_payment.reverse\"],\"description\":\"tokyo\",\"livemode\":false,\"active\":true,\"id\":\"68c61ccbe942880017963f78\",\"object\":\"webhook\"},\"previous_attributes\":{}},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDEfhVfknXHnKQu\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c61ccbe942880017963f79\",\"object\":\"event\",\"type\":\"webhook.created\",\"created_at\":1757813963}', '2025-09-13 19:39:39'),
(51, 'tokyo_69ee6ea1fbb44222', 'order.created', '68c61ed09e2c91001907bc92', '{\"data\":{\"object\":{\"livemode\":false,\"amount\":34717,\"currency\":\"MXN\",\"payment_status\":null,\"amount_refunded\":0,\"split_payment\":null,\"customer_info\":{\"email\":\"fued2@live.com.mx\",\"phone\":\"+526183021446\",\"name\":\"alejandro\",\"corporate\":null,\"customer_id\":null,\"date_of_birth\":null,\"national_id\":null,\"object\":\"customer_info\"},\"shipping_contact\":null,\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDMFAXS6gAE5BDa\"},\"fiscal_entity\":null,\"checkout\":{\"id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"name\":\"ord-2ygDMFAXS6gAE5BDZ\",\"livemode\":false,\"emails_sent\":0,\"success_url\":\"http://localhost/rest2/tokyo/vistas/pago_exitoso.php?ref=tokyo_69ee6ea1fbb44222\",\"failure_url\":\"http://localhost/tokyo/vistas/pago_fallido.php?ref=tokyo_69ee6ea1fbb44222\",\"payments_limit_count\":null,\"paid_payments_count\":0,\"sms_sent\":0,\"status\":\"Issued\",\"type\":\"HostedPayment\",\"recurrent\":false,\"starts_at\":1757743200,\"expires_at\":1758002399,\"allowed_payment_methods\":[\"card\"],\"exclude_card_networks\":[],\"needs_shipping_contact\":false,\"monthly_installments_options\":[],\"monthly_installments_enabled\":false,\"redirection_time\":null,\"force_3ds_flow\":false,\"plan_id\":null,\"metadata\":{},\"can_not_expire\":false,\"three_ds_mode\":null,\"max_failed_retries\":null,\"object\":\"checkout\",\"is_redirect_on_failure\":true,\"slug\":\"8d3ec91eadf8412182e3a7ab0545b0b9\",\"url\":\"https://pay.conekta.com/checkout/8d3ec91eadf8412182e3a7ab0545b0b9\"},\"object\":\"order\",\"id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{\"ref\":\"tokyo_69ee6ea1fbb44222\",\"payment_id\":17,\"context\":{\"tipo\":\"rapido\",\"sede_id\":1,\"corte_id\":82},\"surcharge\":{\"cents\":1717,\"method\":\"card\"}},\"is_refundable\":false,\"processing_mode\":null,\"created_at\":1757814480,\"updated_at\":1757814480,\"line_items\":{\"object\":\"list\",\"has_more\":false,\"total\":6,\"data\":[{\"name\":\"Comisión por método de pago (card)\",\"description\":null,\"unit_price\":1717,\"quantity\":1,\"sku\":\"FEE-CARD\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDX\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Calpico\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"74\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDW\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Bud Light\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"124\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDV\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Alitas\",\"description\":null,\"unit_price\":13500,\"quantity\":1,\"sku\":\"105\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDU\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"3 Quesos\",\"description\":null,\"unit_price\":11500,\"quantity\":1,\"sku\":\"14\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDT\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Aderezo de Chipotle\",\"description\":null,\"unit_price\":1000,\"quantity\":1,\"sku\":\"77\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDS\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}}]},\"shipping_lines\":null,\"tax_lines\":null,\"discount_lines\":null,\"charges\":null},\"previous_attributes\":{}},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDMFDri3jecupTP\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c61ed09e2c91001907bc92\",\"object\":\"event\",\"type\":\"order.created\",\"created_at\":1757814480}', '2025-09-13 19:48:13'),
(52, 'tokyo_69ee6ea1fbb44222', 'order.paid', '68c620525342d8001673a408', '{\"data\":{\"object\":{\"livemode\":false,\"amount\":34717,\"currency\":\"MXN\",\"payment_status\":\"paid\",\"amount_refunded\":0,\"customer_info\":{\"email\":\"fued2@live.com.mx\",\"phone\":\"6183021446\",\"name\":\"fued\",\"corporate\":false,\"customer_id\":\"cus_2yg6xUa6VP1VJJj5V\",\"object\":\"customer_info\"},\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDMFAXS6gAE5BDa\"},\"checkout\":{\"id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"name\":\"ord-2ygDMFAXS6gAE5BDZ\",\"livemode\":false,\"emails_sent\":0,\"success_url\":\"http://localhost/rest2/tokyo/vistas/pago_exitoso.php?ref=tokyo_69ee6ea1fbb44222\",\"failure_url\":\"http://localhost/tokyo/vistas/pago_fallido.php?ref=tokyo_69ee6ea1fbb44222\",\"paid_payments_count\":0,\"sms_sent\":0,\"status\":\"Issued\",\"type\":\"HostedPayment\",\"recurrent\":false,\"starts_at\":1757743200,\"expires_at\":1758002399,\"allowed_payment_methods\":[\"card\"],\"exclude_card_networks\":[],\"needs_shipping_contact\":false,\"monthly_installments_options\":[],\"monthly_installments_enabled\":false,\"force_3ds_flow\":false,\"metadata\":{},\"can_not_expire\":false,\"object\":\"checkout\",\"is_redirect_on_failure\":true,\"slug\":\"8d3ec91eadf8412182e3a7ab0545b0b9\",\"url\":\"https://pay.conekta.com/checkout/8d3ec91eadf8412182e3a7ab0545b0b9\"},\"object\":\"order\",\"id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{\"ref\":\"tokyo_69ee6ea1fbb44222\",\"payment_id\":17,\"context\":{\"tipo\":\"rapido\",\"sede_id\":1,\"corte_id\":82},\"surcharge\":{\"cents\":1717,\"method\":\"card\"}},\"is_refundable\":true,\"created_at\":1757814480,\"updated_at\":1757814866,\"line_items\":{\"object\":\"list\",\"has_more\":false,\"total\":6,\"data\":[{\"name\":\"Comisión por método de pago (card)\",\"unit_price\":1717,\"quantity\":1,\"sku\":\"FEE-CARD\",\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDX\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Calpico\",\"unit_price\":3500,\"quantity\":1,\"sku\":\"74\",\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDW\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Bud Light\",\"unit_price\":3500,\"quantity\":1,\"sku\":\"124\",\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDV\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Alitas\",\"unit_price\":13500,\"quantity\":1,\"sku\":\"105\",\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDU\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"3 Quesos\",\"unit_price\":11500,\"quantity\":1,\"sku\":\"14\",\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDT\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Aderezo de Chipotle\",\"unit_price\":1000,\"quantity\":1,\"sku\":\"77\",\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDS\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}}]},\"charges\":{\"object\":\"list\",\"has_more\":false,\"total\":1,\"data\":[{\"id\":\"68c620515342d8001673a3f6\",\"livemode\":false,\"created_at\":1757814865,\"currency\":\"MXN\",\"device_fingerprint\":\"5794641928f746628bb11b590207e485\",\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDS9BS9NHAPuRuv\"},\"payment_method\":{\"name\":\"fued\",\"exp_month\":\"10\",\"exp_year\":\"27\",\"auth_code\":\"189043\",\"object\":\"card_payment\",\"type\":\"credit\",\"last4\":\"4444\",\"brand\":\"mastercard\",\"issuer\":\"santander\",\"account_type\":\"SANTANDER\",\"country\":\"MX\",\"fraud_indicators\":[],\"antifraud_flag\":\"\",\"three_ds_flow_required\":false},\"object\":\"charge\",\"description\":\"Payment from order\",\"status\":\"paid\",\"amount\":34717,\"paid_at\":1757814865,\"fee\":1717,\"customer_id\":\"cus_2yg6xUa6VP1VJJj5V\",\"order_id\":\"ord_2ygDMFAXS6gAE5BDZ\"}]}},\"previous_attributes\":{}},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDS9r2uTYyXdB8m\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c620525342d8001673a408\",\"object\":\"event\",\"type\":\"order.paid\",\"created_at\":1757814866}', '2025-09-13 19:54:35'),
(53, NULL, 'charge.created', '68c620515342d8001673a3ff', '{\"data\":{\"object\":{\"id\":\"68c620515342d8001673a3f6\",\"livemode\":false,\"created_at\":1757814865,\"currency\":\"MXN\",\"device_fingerprint\":\"5794641928f746628bb11b590207e485\",\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDS9BS9NHAPuRuv\"},\"payment_method\":{\"name\":\"fued\",\"exp_month\":\"10\",\"exp_year\":\"27\",\"object\":\"card_payment\",\"type\":\"credit\",\"last4\":\"4444\",\"brand\":\"mastercard\",\"issuer\":\"santander\",\"account_type\":\"SANTANDER\",\"country\":\"MX\",\"fraud_indicators\":[],\"antifraud_flag\":\"\",\"three_ds_flow_required\":false},\"object\":\"charge\",\"description\":\"Payment from order\",\"status\":\"pending_payment\",\"amount\":34717,\"fee\":1717,\"customer_id\":\"cus_2yg6xUa6VP1VJJj5V\",\"order_id\":\"ord_2ygDMFAXS6gAE5BDZ\"},\"previous_attributes\":{}},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDS9LFRoY9LCJE5\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c620515342d8001673a3ff\",\"object\":\"event\",\"type\":\"charge.created\",\"created_at\":1757814865}', '2025-09-13 19:54:35'),
(54, 'tokyo_69ee6ea1fbb44222', 'order.updated', '68c62051f3f58d00164bd5e2', '{\"data\":{\"object\":{\"livemode\":false,\"amount\":34717,\"currency\":\"MXN\",\"payment_status\":null,\"amount_refunded\":0,\"split_payment\":false,\"customer_info\":{\"email\":\"fued2@live.com.mx\",\"phone\":\"6183021446\",\"name\":\"fued\",\"corporate\":false,\"customer_id\":\"cus_2yg6xUa6VP1VJJj5V\",\"date_of_birth\":null,\"national_id\":null,\"object\":\"customer_info\",\"customer_custom_reference\":null},\"shipping_contact\":null,\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDMFAXS6gAE5BDa\"},\"fiscal_entity\":null,\"checkout\":{\"id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"name\":\"ord-2ygDMFAXS6gAE5BDZ\",\"livemode\":false,\"emails_sent\":0,\"success_url\":\"http://localhost/rest2/tokyo/vistas/pago_exitoso.php?ref=tokyo_69ee6ea1fbb44222\",\"failure_url\":\"http://localhost/tokyo/vistas/pago_fallido.php?ref=tokyo_69ee6ea1fbb44222\",\"payments_limit_count\":null,\"paid_payments_count\":0,\"sms_sent\":0,\"status\":\"Issued\",\"type\":\"HostedPayment\",\"recurrent\":false,\"starts_at\":1757743200,\"expires_at\":1758002399,\"allowed_payment_methods\":[\"card\"],\"exclude_card_networks\":[],\"needs_shipping_contact\":false,\"monthly_installments_options\":[],\"monthly_installments_enabled\":false,\"redirection_time\":null,\"force_3ds_flow\":false,\"plan_id\":null,\"metadata\":{},\"can_not_expire\":false,\"three_ds_mode\":null,\"max_failed_retries\":null,\"object\":\"checkout\",\"is_redirect_on_failure\":true,\"slug\":\"8d3ec91eadf8412182e3a7ab0545b0b9\",\"url\":\"https://pay.conekta.com/checkout/8d3ec91eadf8412182e3a7ab0545b0b9\"},\"object\":\"order\",\"id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{\"ref\":\"tokyo_69ee6ea1fbb44222\",\"payment_id\":17,\"context\":{\"tipo\":\"rapido\",\"sede_id\":1,\"corte_id\":82},\"surcharge\":{\"cents\":1717,\"method\":\"card\"}},\"is_refundable\":false,\"processing_mode\":null,\"created_at\":1757814480,\"updated_at\":1757814865,\"line_items\":{\"object\":\"list\",\"has_more\":false,\"total\":6,\"data\":[{\"name\":\"Comisión por método de pago (card)\",\"description\":null,\"unit_price\":1717,\"quantity\":1,\"sku\":\"FEE-CARD\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDX\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Calpico\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"74\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDW\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Bud Light\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"124\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDV\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Alitas\",\"description\":null,\"unit_price\":13500,\"quantity\":1,\"sku\":\"105\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDU\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"3 Quesos\",\"description\":null,\"unit_price\":11500,\"quantity\":1,\"sku\":\"14\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDT\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Aderezo de Chipotle\",\"description\":null,\"unit_price\":1000,\"quantity\":1,\"sku\":\"77\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDS\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}}]},\"shipping_lines\":null,\"tax_lines\":null,\"discount_lines\":null,\"charges\":null},\"previous_attributes\":{}},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDS9ckPXPdM8c8U\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c62051f3f58d00164bd5e2\",\"object\":\"event\",\"type\":\"order.updated\",\"created_at\":1757814865}', '2025-09-13 19:54:36'),
(55, 'tokyo_69ee6ea1fbb44222', 'order.updated', '68c62051ea08590015a7ecf9', '{\"data\":{\"object\":{\"livemode\":false,\"amount\":34717,\"currency\":\"MXN\",\"payment_status\":null,\"amount_refunded\":0,\"split_payment\":null,\"customer_info\":{\"email\":\"fued2@live.com.mx\",\"phone\":\"6183021446\",\"name\":\"fued\",\"corporate\":false,\"customer_id\":\"cus_2yg6xUa6VP1VJJj5V\",\"date_of_birth\":null,\"national_id\":null,\"object\":\"customer_info\",\"customer_custom_reference\":null},\"shipping_contact\":null,\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDMFAXS6gAE5BDa\"},\"fiscal_entity\":null,\"checkout\":{\"id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"name\":\"ord-2ygDMFAXS6gAE5BDZ\",\"livemode\":false,\"emails_sent\":0,\"success_url\":\"http://localhost/rest2/tokyo/vistas/pago_exitoso.php?ref=tokyo_69ee6ea1fbb44222\",\"failure_url\":\"http://localhost/tokyo/vistas/pago_fallido.php?ref=tokyo_69ee6ea1fbb44222\",\"payments_limit_count\":null,\"paid_payments_count\":0,\"sms_sent\":0,\"status\":\"Issued\",\"type\":\"HostedPayment\",\"recurrent\":false,\"starts_at\":1757743200,\"expires_at\":1758002399,\"allowed_payment_methods\":[\"card\"],\"exclude_card_networks\":[],\"needs_shipping_contact\":false,\"monthly_installments_options\":[],\"monthly_installments_enabled\":false,\"redirection_time\":null,\"force_3ds_flow\":false,\"plan_id\":null,\"metadata\":{},\"can_not_expire\":false,\"three_ds_mode\":null,\"max_failed_retries\":null,\"object\":\"checkout\",\"is_redirect_on_failure\":true,\"slug\":\"8d3ec91eadf8412182e3a7ab0545b0b9\",\"url\":\"https://pay.conekta.com/checkout/8d3ec91eadf8412182e3a7ab0545b0b9\"},\"object\":\"order\",\"id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{\"ref\":\"tokyo_69ee6ea1fbb44222\",\"payment_id\":17,\"context\":{\"tipo\":\"rapido\",\"sede_id\":1,\"corte_id\":82},\"surcharge\":{\"cents\":1717,\"method\":\"card\"}},\"is_refundable\":false,\"processing_mode\":null,\"created_at\":1757814480,\"updated_at\":1757814865,\"line_items\":{\"object\":\"list\",\"has_more\":false,\"total\":6,\"data\":[{\"name\":\"Comisión por método de pago (card)\",\"description\":null,\"unit_price\":1717,\"quantity\":1,\"sku\":\"FEE-CARD\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDX\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Calpico\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"74\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDW\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Bud Light\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"124\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDV\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Alitas\",\"description\":null,\"unit_price\":13500,\"quantity\":1,\"sku\":\"105\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDU\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"3 Quesos\",\"description\":null,\"unit_price\":11500,\"quantity\":1,\"sku\":\"14\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDT\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}},{\"name\":\"Aderezo de Chipotle\",\"description\":null,\"unit_price\":1000,\"quantity\":1,\"sku\":\"77\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDS\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{},\"antifraud_info\":{}}]},\"shipping_lines\":null,\"tax_lines\":null,\"discount_lines\":null,\"charges\":null},\"previous_attributes\":{}},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDS97DPFzKuMWeF\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c62051ea08590015a7ecf9\",\"object\":\"event\",\"type\":\"order.updated\",\"created_at\":1757814865}', '2025-09-13 19:54:36'),
(56, NULL, 'charge.paid', '68c620515342d8001673a402', '{\"data\":{\"object\":{\"id\":\"68c620515342d8001673a3f6\",\"livemode\":false,\"created_at\":1757814865,\"currency\":\"MXN\",\"device_fingerprint\":\"5794641928f746628bb11b590207e485\",\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDS9BS9NHAPuRuv\"},\"payment_method\":{\"name\":\"fued\",\"exp_month\":\"10\",\"exp_year\":\"27\",\"auth_code\":\"189043\",\"object\":\"card_payment\",\"type\":\"credit\",\"last4\":\"4444\",\"brand\":\"mastercard\",\"issuer\":\"santander\",\"account_type\":\"SANTANDER\",\"country\":\"MX\",\"fraud_indicators\":[],\"antifraud_flag\":\"\",\"three_ds_flow_required\":false},\"object\":\"charge\",\"description\":\"Payment from order\",\"status\":\"paid\",\"amount\":34717,\"paid_at\":1757814865,\"fee\":1717,\"customer_id\":\"cus_2yg6xUa6VP1VJJj5V\",\"order_id\":\"ord_2ygDMFAXS6gAE5BDZ\"},\"previous_attributes\":{\"payment_method\":{}}},\"livemode\":false,\"webhook_status\":\"pending\",\"webhook_logs\":[{\"id\":\"webhl_2ygDS9zn8NUYiKCyi\",\"url\":\"https://4e331352fe1e.ngrok-free.app/rest2/tokyo/api/checkout/conekta_webhook.php\",\"failed_attempts\":0,\"last_http_response_status\":-1,\"response_data\":null,\"object\":\"webhook_log\",\"last_attempted_at\":0}],\"id\":\"68c620515342d8001673a402\",\"object\":\"event\",\"type\":\"charge.paid\",\"created_at\":1757814866}', '2025-09-13 19:54:36');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `conekta_payments`
--

CREATE TABLE `conekta_payments` (
  `id` int(11) NOT NULL,
  `reference` varchar(64) NOT NULL,
  `venta_id` int(11) DEFAULT NULL,
  `customer_name` varchar(150) DEFAULT NULL,
  `customer_email` varchar(150) DEFAULT NULL,
  `customer_phone` varchar(30) DEFAULT NULL,
  `amount` int(11) NOT NULL,
  `currency` varchar(8) NOT NULL DEFAULT 'MXN',
  `status` enum('pending','paid','expired','canceled','failed') NOT NULL DEFAULT 'pending',
  `payment_method` varchar(32) DEFAULT NULL,
  `conekta_order_id` varchar(64) DEFAULT NULL,
  `conekta_checkout_id` varchar(64) DEFAULT NULL,
  `checkout_url` text DEFAULT NULL,
  `cart_snapshot` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`cart_snapshot`)),
  `metadata` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`metadata`)),
  `raw_order` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`raw_order`)),
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `conekta_payments`
--

INSERT INTO `conekta_payments` (`id`, `reference`, `venta_id`, `customer_name`, `customer_email`, `customer_phone`, `amount`, `currency`, `status`, `payment_method`, `conekta_order_id`, `conekta_checkout_id`, `checkout_url`, `cart_snapshot`, `metadata`, `raw_order`, `created_at`, `updated_at`) VALUES
(17, 'tokyo_69ee6ea1fbb44222', 344, 'alejandro', 'fued2@live.com.mx', '+526183021446', 34717, 'MXN', 'paid', NULL, 'ord_2ygDMFAXS6gAE5BDZ', '8d3ec91e-adf8-4121-82e3-a7ab0545b0b9', 'https://pay.conekta.com/checkout/8d3ec91eadf8412182e3a7ab0545b0b9', '{\"items\":{\"77\":1,\"14\":1,\"105\":1,\"124\":1,\"74\":1},\"fee\":{\"method\":\"card\",\"surcharge_mx\":17.1724827184143,\"surcharge_cents\":1717}}', '{\"ref\":\"tokyo_69ee6ea1fbb44222\",\"context\":{\"tipo\":\"rapido\",\"mesa_id\":null,\"repartidor_id\":null,\"usuario_id\":1,\"sede_id\":1,\"corte_id\":82,\"observacion\":null},\"surcharge\":{\"mx\":17.1724827184143,\"cents\":1717,\"method\":\"card\",\"meta\":{\"method\":\"card\",\"rate\":0.034,\"fixed\":3,\"iva\":0.16,\"min_fee\":5.4,\"grossed_up\":true}}}', '{\"livemode\":false,\"amount\":34717,\"currency\":\"MXN\",\"payment_status\":null,\"amount_refunded\":0,\"split_payment\":null,\"customer_info\":{\"email\":\"fued2@live.com.mx\",\"phone\":\"+526183021446\",\"name\":\"alejandro\",\"corporate\":null,\"customer_id\":null,\"date_of_birth\":null,\"national_id\":null,\"object\":\"customer_info\"},\"shipping_contact\":null,\"channel\":{\"segment\":\"Checkout\",\"checkout_request_id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"checkout_request_type\":\"HostedPayment\",\"id\":\"channel_2ygDMFAXS6gAE5BDa\"},\"fiscal_entity\":null,\"checkout\":{\"id\":\"8d3ec91e-adf8-4121-82e3-a7ab0545b0b9\",\"name\":\"ord-2ygDMFAXS6gAE5BDZ\",\"livemode\":false,\"emails_sent\":0,\"success_url\":\"http:\\/\\/localhost\\/rest2\\/tokyo\\/vistas\\/pago_exitoso.php?ref=tokyo_69ee6ea1fbb44222\",\"failure_url\":\"http:\\/\\/localhost\\/tokyo\\/vistas\\/pago_fallido.php?ref=tokyo_69ee6ea1fbb44222\",\"payments_limit_count\":null,\"paid_payments_count\":0,\"sms_sent\":0,\"status\":\"Issued\",\"type\":\"HostedPayment\",\"recurrent\":false,\"starts_at\":1757743200,\"expires_at\":1758002399,\"allowed_payment_methods\":[\"card\"],\"exclude_card_networks\":[],\"needs_shipping_contact\":false,\"monthly_installments_options\":[],\"monthly_installments_enabled\":false,\"redirection_time\":null,\"force_3ds_flow\":false,\"plan_id\":null,\"metadata\":[],\"can_not_expire\":false,\"three_ds_mode\":null,\"max_failed_retries\":null,\"object\":\"checkout\",\"is_redirect_on_failure\":true,\"slug\":\"8d3ec91eadf8412182e3a7ab0545b0b9\",\"url\":\"https:\\/\\/pay.conekta.com\\/checkout\\/8d3ec91eadf8412182e3a7ab0545b0b9\"},\"object\":\"order\",\"id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":{\"ref\":\"tokyo_69ee6ea1fbb44222\",\"payment_id\":17,\"context\":{\"tipo\":\"rapido\",\"sede_id\":1,\"corte_id\":82},\"surcharge\":{\"cents\":1717,\"method\":\"card\"}},\"is_refundable\":false,\"processing_mode\":null,\"created_at\":1757814480,\"updated_at\":1757814480,\"line_items\":{\"object\":\"list\",\"has_more\":false,\"total\":6,\"data\":[{\"name\":\"Comisión por método de pago (card)\",\"description\":null,\"unit_price\":1717,\"quantity\":1,\"sku\":\"FEE-CARD\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDX\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":[],\"antifraud_info\":[]},{\"name\":\"Calpico\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"74\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDW\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":[],\"antifraud_info\":[]},{\"name\":\"Bud Light\",\"description\":null,\"unit_price\":3500,\"quantity\":1,\"sku\":\"124\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDV\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":[],\"antifraud_info\":[]},{\"name\":\"Alitas\",\"description\":null,\"unit_price\":13500,\"quantity\":1,\"sku\":\"105\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDU\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":[],\"antifraud_info\":[]},{\"name\":\"3 Quesos\",\"description\":null,\"unit_price\":11500,\"quantity\":1,\"sku\":\"14\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDT\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":[],\"antifraud_info\":[]},{\"name\":\"Aderezo de Chipotle\",\"description\":null,\"unit_price\":1000,\"quantity\":1,\"sku\":\"77\",\"tags\":null,\"brand\":null,\"type\":null,\"object\":\"line_item\",\"id\":\"line_item_2ygDMFAXS6gAE5BDS\",\"parent_id\":\"ord_2ygDMFAXS6gAE5BDZ\",\"metadata\":[],\"antifraud_info\":[]}]},\"shipping_lines\":null,\"tax_lines\":null,\"discount_lines\":null,\"charges\":null}', '2025-09-13 19:47:58', '2025-09-13 19:55:25');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `cortes_almacen`
--

CREATE TABLE `cortes_almacen` (
  `id` int(11) NOT NULL,
  `fecha_inicio` datetime DEFAULT current_timestamp(),
  `fecha_fin` datetime DEFAULT NULL,
  `usuario_abre_id` int(11) DEFAULT NULL,
  `usuario_cierra_id` int(11) DEFAULT NULL,
  `observaciones` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf32 COLLATE=utf32_bin;

--
-- Volcado de datos para la tabla `cortes_almacen`
--

INSERT INTO `cortes_almacen` (`id`, `fecha_inicio`, `fecha_fin`, `usuario_abre_id`, `usuario_cierra_id`, `observaciones`) VALUES
(1, '2025-09-13 17:50:38', NULL, 1, NULL, NULL);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `cortes_almacen_detalle`
--

CREATE TABLE `cortes_almacen_detalle` (
  `id` int(11) NOT NULL,
  `corte_id` int(11) DEFAULT NULL,
  `insumo_id` int(11) DEFAULT NULL,
  `existencia_inicial` decimal(10,2) DEFAULT NULL,
  `entradas` decimal(10,2) DEFAULT NULL,
  `salidas` decimal(10,2) DEFAULT NULL,
  `mermas` decimal(10,2) DEFAULT NULL,
  `existencia_final` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf32 COLLATE=utf32_bin;

--
-- Volcado de datos para la tabla `cortes_almacen_detalle`
--

INSERT INTO `cortes_almacen_detalle` (`id`, `corte_id`, `insumo_id`, `existencia_inicial`, `entradas`, `salidas`, `mermas`, `existencia_final`) VALUES
(1, 1, 1, 20240.00, 0.00, 0.00, 0.00, NULL),
(2, 1, 2, 29974.00, 0.00, 0.00, 0.00, NULL),
(3, 1, 3, 30000.00, 0.00, 0.00, 0.00, NULL),
(4, 1, 4, 29999.00, 0.00, 0.00, 0.00, NULL),
(5, 1, 7, 30000.00, 0.00, 0.00, 0.00, NULL),
(6, 1, 8, 29370.00, 0.00, 0.00, 0.00, NULL),
(7, 1, 9, 29790.00, 0.00, 0.00, 0.00, NULL),
(8, 1, 10, 29605.00, 0.00, 0.00, 0.00, NULL),
(9, 1, 11, 29980.00, 0.00, 0.00, 0.00, NULL),
(10, 1, 12, 28600.00, 0.00, 0.00, 0.00, NULL),
(11, 1, 13, 30000.00, 0.00, 0.00, 0.00, NULL),
(12, 1, 14, 29570.00, 0.00, 0.00, 0.00, NULL),
(13, 1, 15, 29994.00, 0.00, 0.00, 0.00, NULL),
(14, 1, 16, 29989.00, 0.00, 0.00, 0.00, NULL),
(15, 1, 17, 30000.00, 0.00, 0.00, 0.00, NULL),
(16, 1, 18, 30000.00, 0.00, 0.00, 0.00, NULL),
(17, 1, 19, 29992.00, 0.00, 0.00, 0.00, NULL),
(18, 1, 20, 29996.00, 0.00, 0.00, 0.00, NULL),
(19, 1, 21, 29940.00, 0.00, 0.00, 0.00, NULL),
(20, 1, 22, 30000.00, 0.00, 0.00, 0.00, NULL),
(21, 1, 23, 29990.00, 0.00, 0.00, 0.00, NULL),
(22, 1, 24, 28670.00, 0.00, 0.00, 0.00, NULL),
(23, 1, 25, 30000.00, 0.00, 0.00, 0.00, NULL),
(24, 1, 26, 30000.00, 0.00, 0.00, 0.00, NULL),
(25, 1, 27, 30000.00, 0.00, 0.00, 0.00, NULL),
(26, 1, 28, 29750.00, 0.00, 0.00, 0.00, NULL),
(27, 1, 29, 30000.00, 0.00, 0.00, 0.00, NULL),
(28, 1, 30, 30000.00, 0.00, 0.00, 0.00, NULL),
(29, 1, 31, 30000.00, 0.00, 0.00, 0.00, NULL),
(30, 1, 32, 29985.00, 0.00, 0.00, 0.00, NULL),
(31, 1, 33, 29830.00, 0.00, 0.00, 0.00, NULL),
(32, 1, 34, 30000.00, 0.00, 0.00, 0.00, NULL),
(33, 1, 35, 30000.00, 0.00, 0.00, 0.00, NULL),
(34, 1, 36, 28340.00, 0.00, 0.00, 0.00, NULL),
(35, 1, 37, 30000.00, 0.00, 0.00, 0.00, NULL),
(36, 1, 38, 30000.00, 0.00, 0.00, 0.00, NULL),
(37, 1, 39, 30000.00, 0.00, 0.00, 0.00, NULL),
(38, 1, 40, 30000.00, 0.00, 0.00, 0.00, NULL),
(39, 1, 41, 30000.00, 0.00, 0.00, 0.00, NULL),
(40, 1, 42, 30000.00, 0.00, 0.00, 0.00, NULL),
(41, 1, 43, 30000.00, 0.00, 0.00, 0.00, NULL),
(42, 1, 44, 30000.00, 0.00, 0.00, 0.00, NULL),
(43, 1, 45, 29960.00, 0.00, 0.00, 0.00, NULL),
(44, 1, 46, 29970.00, 0.00, 0.00, 0.00, NULL),
(45, 1, 47, 30000.00, 0.00, 0.00, 0.00, NULL),
(46, 1, 48, 29940.00, 0.00, 0.00, 0.00, NULL),
(47, 1, 49, 30000.00, 0.00, 0.00, 0.00, NULL),
(48, 1, 50, 30000.00, 0.00, 0.00, 0.00, NULL),
(49, 1, 51, 30000.00, 0.00, 0.00, 0.00, NULL),
(50, 1, 52, 30000.00, 0.00, 0.00, 0.00, NULL),
(51, 1, 53, 30000.00, 0.00, 0.00, 0.00, NULL),
(52, 1, 54, 30000.00, 0.00, 0.00, 0.00, NULL),
(53, 1, 55, 30000.00, 0.00, 0.00, 0.00, NULL),
(54, 1, 56, 29995.00, 0.00, 0.00, 0.00, NULL),
(55, 1, 57, 30000.00, 0.00, 0.00, 0.00, NULL),
(56, 1, 59, 30000.00, 0.00, 0.00, 0.00, NULL),
(57, 1, 60, 30000.00, 0.00, 0.00, 0.00, NULL),
(58, 1, 61, 29880.00, 0.00, 0.00, 0.00, NULL),
(59, 1, 62, 30000.00, 0.00, 0.00, 0.00, NULL),
(60, 1, 63, 29990.00, 0.00, 0.00, 0.00, NULL),
(61, 1, 64, 30000.00, 0.00, 0.00, 0.00, NULL),
(62, 1, 65, 30000.00, 0.00, 0.00, 0.00, NULL),
(63, 1, 66, 28560.00, 0.00, 0.00, 0.00, NULL),
(64, 1, 67, 30000.00, 0.00, 0.00, 0.00, NULL),
(65, 1, 69, 30000.00, 0.00, 0.00, 0.00, NULL),
(66, 1, 70, 30000.00, 0.00, 0.00, 0.00, NULL),
(67, 1, 71, 30000.00, 0.00, 0.00, 0.00, NULL),
(68, 1, 72, 29986.00, 0.00, 0.00, 0.00, NULL),
(69, 1, 73, 30000.00, 0.00, 0.00, 0.00, NULL),
(70, 1, 74, 30000.00, 0.00, 0.00, 0.00, NULL),
(71, 1, 75, 30000.00, 0.00, 0.00, 0.00, NULL),
(72, 1, 76, 30000.00, 0.00, 0.00, 0.00, NULL),
(73, 1, 77, 30000.00, 0.00, 0.00, 0.00, NULL),
(74, 1, 78, 29980.00, 0.00, 0.00, 0.00, NULL),
(75, 1, 79, 30000.00, 0.00, 0.00, 0.00, NULL),
(76, 1, 80, 29925.00, 0.00, 0.00, 0.00, NULL),
(77, 1, 81, 29960.00, 0.00, 0.00, 0.00, NULL),
(78, 1, 82, 30000.00, 0.00, 0.00, 0.00, NULL),
(79, 1, 83, 30000.00, 0.00, 0.00, 0.00, NULL),
(80, 1, 85, 30000.00, 0.00, 0.00, 0.00, NULL),
(81, 1, 86, 30000.00, 0.00, 0.00, 0.00, NULL),
(82, 1, 87, 28995.00, 0.00, 0.00, 0.00, NULL),
(83, 1, 88, 29920.00, 0.00, 0.00, 0.00, NULL),
(84, 1, 89, 29960.00, 0.00, 0.00, 0.00, NULL),
(85, 1, 90, 28325.00, 0.00, 0.00, 0.00, NULL),
(86, 1, 91, 29850.00, 0.00, 0.00, 0.00, NULL),
(87, 1, 92, 29970.00, 0.00, 0.00, 0.00, NULL),
(88, 1, 93, 30000.00, 0.00, 0.00, 0.00, NULL),
(89, 1, 94, 29840.00, 0.00, 0.00, 0.00, NULL),
(90, 1, 95, 30000.00, 0.00, 0.00, 0.00, NULL),
(91, 1, 96, 30000.00, 0.00, 0.00, 0.00, NULL),
(92, 1, 97, 30000.00, 0.00, 0.00, 0.00, NULL),
(93, 1, 98, 30000.00, 0.00, 0.00, 0.00, NULL),
(94, 1, 99, 29990.00, 0.00, 0.00, 0.00, NULL),
(95, 1, 101, 29999.00, 0.00, 0.00, 0.00, NULL),
(96, 1, 102, 30000.00, 0.00, 0.00, 0.00, NULL),
(97, 1, 103, 30000.00, 0.00, 0.00, 0.00, NULL),
(98, 1, 104, 29996.00, 0.00, 0.00, 0.00, NULL),
(99, 1, 105, 29998.00, 0.00, 0.00, 0.00, NULL),
(100, 1, 106, 30000.00, 0.00, 0.00, 0.00, NULL),
(101, 1, 107, 30000.00, 0.00, 0.00, 0.00, NULL),
(102, 1, 108, 30000.00, 0.00, 0.00, 0.00, NULL),
(103, 1, 109, 29450.00, 0.00, 0.00, 0.00, NULL),
(104, 1, 110, 30000.00, 0.00, 0.00, 0.00, NULL),
(105, 1, 111, 30000.00, 0.00, 0.00, 0.00, NULL),
(106, 1, 112, 30000.00, 0.00, 0.00, 0.00, NULL),
(107, 1, 113, 29910.00, 0.00, 0.00, 0.00, NULL),
(108, 1, 114, 30000.00, 0.00, 0.00, 0.00, NULL),
(109, 1, 115, 29750.00, 0.00, 0.00, 0.00, NULL),
(110, 1, 116, 30000.00, 0.00, 0.00, 0.00, NULL),
(111, 1, 117, 29700.00, 0.00, 0.00, 0.00, NULL),
(112, 1, 118, 29990.00, 0.00, 0.00, 0.00, NULL),
(113, 1, 119, 30000.00, 0.00, 0.00, 0.00, NULL),
(114, 1, 120, 0.00, 0.00, 0.00, 0.00, NULL),
(115, 1, 121, 0.00, 0.00, 0.00, 0.00, NULL),
(116, 1, 122, 0.00, 0.00, 0.00, 0.00, NULL),
(117, 1, 123, 0.00, 0.00, 0.00, 0.00, NULL),
(118, 1, 124, 0.00, 0.00, 0.00, 0.00, NULL),
(119, 1, 125, 0.00, 0.00, 0.00, 0.00, NULL),
(120, 1, 126, 0.00, 0.00, 0.00, 0.00, NULL),
(121, 1, 127, 0.00, 0.00, 0.00, 0.00, NULL),
(122, 1, 128, 0.00, 0.00, 0.00, 0.00, NULL),
(123, 1, 129, 0.00, 0.00, 0.00, 0.00, NULL),
(124, 1, 130, 0.00, 0.00, 0.00, 0.00, NULL),
(125, 1, 131, 0.00, 0.00, 0.00, 0.00, NULL),
(126, 1, 132, 0.00, 0.00, 0.00, 0.00, NULL),
(127, 1, 133, 0.00, 0.00, 0.00, 0.00, NULL),
(128, 1, 134, 0.00, 0.00, 0.00, 0.00, NULL),
(129, 1, 135, 0.00, 0.00, 0.00, 0.00, NULL),
(130, 1, 136, 0.00, 0.00, 0.00, 0.00, NULL),
(131, 1, 137, 0.00, 0.00, 0.00, 0.00, NULL),
(132, 1, 138, 0.00, 0.00, 0.00, 0.00, NULL),
(133, 1, 139, 0.00, 0.00, 0.00, 0.00, NULL),
(134, 1, 140, 0.00, 0.00, 0.00, 0.00, NULL),
(135, 1, 141, 0.00, 0.00, 0.00, 0.00, NULL),
(136, 1, 142, 0.00, 0.00, 0.00, 0.00, NULL),
(137, 1, 143, 0.00, 0.00, 0.00, 0.00, NULL),
(138, 1, 144, 0.00, 0.00, 0.00, 0.00, NULL),
(139, 1, 145, 0.00, 0.00, 0.00, 0.00, NULL),
(140, 1, 146, 0.00, 0.00, 0.00, 0.00, NULL),
(141, 1, 147, 0.00, 0.00, 0.00, 0.00, NULL),
(142, 1, 148, 0.00, 0.00, 0.00, 0.00, NULL),
(143, 1, 149, 0.00, 0.00, 0.00, 0.00, NULL),
(144, 1, 150, 0.00, 0.00, 0.00, 0.00, NULL),
(145, 1, 151, 0.00, 0.00, 0.00, 0.00, NULL),
(146, 1, 152, 0.00, 0.00, 0.00, 0.00, NULL),
(147, 1, 153, 0.00, 0.00, 0.00, 0.00, NULL),
(148, 1, 154, 0.00, 0.00, 0.00, 0.00, NULL),
(149, 1, 155, 0.00, 0.00, 0.00, 0.00, NULL),
(150, 1, 156, 0.00, 0.00, 0.00, 0.00, NULL),
(151, 1, 157, 0.00, 0.00, 0.00, 0.00, NULL),
(152, 1, 158, 0.00, 0.00, 0.00, 0.00, NULL),
(153, 1, 159, 0.00, 0.00, 0.00, 0.00, NULL),
(154, 1, 160, 0.00, 0.00, 0.00, 0.00, NULL),
(155, 1, 161, 0.00, 0.00, 0.00, 0.00, NULL),
(156, 1, 162, 0.00, 0.00, 0.00, 0.00, NULL),
(157, 1, 163, 0.00, 0.00, 0.00, 0.00, NULL),
(158, 1, 164, 0.00, 0.00, 0.00, 0.00, NULL),
(159, 1, 165, 0.00, 0.00, 0.00, 0.00, NULL),
(160, 1, 166, 0.00, 0.00, 0.00, 0.00, NULL),
(161, 1, 167, 0.00, 0.00, 0.00, 0.00, NULL),
(162, 1, 168, 0.00, 0.00, 0.00, 0.00, NULL),
(163, 1, 169, 0.00, 0.00, 0.00, 0.00, NULL),
(164, 1, 170, 0.00, 0.00, 0.00, 0.00, NULL),
(165, 1, 171, 0.00, 0.00, 0.00, 0.00, NULL),
(166, 1, 172, 0.00, 0.00, 0.00, 0.00, NULL),
(167, 1, 173, 0.00, 0.00, 0.00, 0.00, NULL),
(168, 1, 174, 0.00, 0.00, 0.00, 0.00, NULL),
(169, 1, 175, 0.00, 0.00, 0.00, 0.00, NULL),
(170, 1, 176, 0.00, 0.00, 0.00, 0.00, NULL),
(171, 1, 177, 0.00, 0.00, 0.00, 0.00, NULL),
(172, 1, 178, 0.00, 0.00, 0.00, 0.00, NULL),
(173, 1, 179, 0.00, 0.00, 0.00, 0.00, NULL),
(174, 1, 180, 0.00, 0.00, 0.00, 0.00, NULL),
(175, 1, 181, 0.00, 0.00, 0.00, 0.00, NULL),
(176, 1, 182, -40.00, 0.00, 0.00, 0.00, NULL),
(177, 1, 183, 0.00, 0.00, 0.00, 0.00, NULL),
(178, 1, 184, 0.00, 0.00, 0.00, 0.00, NULL),
(179, 1, 185, 0.00, 0.00, 0.00, 0.00, NULL),
(180, 1, 186, 0.00, 0.00, 0.00, 0.00, NULL),
(181, 1, 187, 0.00, 0.00, 0.00, 0.00, NULL),
(182, 1, 188, 0.00, 0.00, 0.00, 0.00, NULL),
(183, 1, 189, 0.00, 0.00, 0.00, 0.00, NULL),
(184, 1, 190, 0.00, 0.00, 0.00, 0.00, NULL),
(185, 1, 191, 0.00, 0.00, 0.00, 0.00, NULL);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `corte_caja`
--

CREATE TABLE `corte_caja` (
  `id` int(11) NOT NULL,
  `usuario_id` int(11) NOT NULL,
  `fecha_inicio` datetime NOT NULL DEFAULT current_timestamp(),
  `folio_inicio` int(11) DEFAULT NULL,
  `folio_fin` int(11) DEFAULT NULL,
  `total_folios` int(11) NOT NULL DEFAULT 0,
  `fecha_fin` datetime DEFAULT NULL,
  `total` decimal(10,2) DEFAULT NULL,
  `observaciones` text DEFAULT NULL,
  `fondo_inicial` decimal(10,2) DEFAULT 0.00
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `corte_caja`
--

INSERT INTO `corte_caja` (`id`, `usuario_id`, `fecha_inicio`, `folio_inicio`, `folio_fin`, `total_folios`, `fecha_fin`, `total`, `observaciones`, `fondo_inicial`) VALUES
(82, 1, '2025-09-14 03:45:56', 2066, 2068, 2, '2025-09-13 20:25:38', 1000.00, '', 1000.00);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `corte_caja_historial`
--

CREATE TABLE `corte_caja_historial` (
  `id` int(11) NOT NULL,
  `corte_id` int(11) NOT NULL,
  `usuario_id` int(11) NOT NULL,
  `fecha` datetime DEFAULT current_timestamp(),
  `total` decimal(10,2) DEFAULT 0.00,
  `observaciones` text DEFAULT NULL,
  `datos_json` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`datos_json`))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `corte_caja_historial`
--

INSERT INTO `corte_caja_historial` (`id`, `corte_id`, `usuario_id`, `fecha`, `total`, `observaciones`, `datos_json`) VALUES
(18, 82, 1, '2025-09-13 20:12:25', 2226.17, '', '{\"efectivo\":{\"productos\":379,\"total\":379},\"boucher\":{\"productos\":347.17,\"total\":347.17},\"total_productos\":726.1700000000001,\"total_propina_efectivo\":0,\"total_propina_cheque\":0,\"total_propina_tarjeta\":0,\"total_propinas\":0,\"total_descuento_promos\":105,\"totalEsperado\":726.1700000000001,\"fondo\":1000,\"total_depositos\":500,\"total_retiros\":0,\"totalFinal\":2226.17,\"corte_id\":82,\"total_meseros\":[{\"nombre\":\"alinne Guadalupe Gurrola ramirez\",\"total\":0},{\"nombre\":\"gilberto ozuna carrillo\",\"total\":0},{\"nombre\":\"Javier Emanuel lopez lozano\",\"total\":0},{\"nombre\":\"Jesus\",\"total\":0},{\"nombre\":\"juan hernesto ortega Almanza\",\"total\":0},{\"nombre\":\"Mesas general\",\"total\":0}],\"total_rapido\":347.17,\"total_repartidor\":[{\"nombre\":\"Didi\",\"total\":0},{\"nombre\":\"Rappi\",\"total\":0},{\"nombre\":\"Repartidor casa\",\"total\":379},{\"nombre\":\"Uber\",\"total\":0}],\"fecha_inicio\":\"2025-09-14 03:45:56\",\"folio_inicio\":2066,\"folio_fin\":2067,\"total_folios\":2,\"total_bruto\":726.17,\"total_descuentos\":0,\"total_esperado\":726.17,\"esperado_efectivo\":379,\"esperado_boucher\":347.17,\"esperado_cheque\":0}');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `desglose_corte`
--

CREATE TABLE `desglose_corte` (
  `id` int(11) NOT NULL,
  `corte_id` int(11) NOT NULL,
  `denominacion` decimal(10,2) NOT NULL,
  `cantidad` int(11) NOT NULL,
  `tipo_pago` enum('efectivo','boucher','cheque') DEFAULT 'efectivo',
  `denominacion_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `desglose_corte`
--

INSERT INTO `desglose_corte` (`id`, `corte_id`, `denominacion`, `cantidad`, `tipo_pago`, `denominacion_id`) VALUES
(331, 82, 1.00, 347, 'boucher', 12),
(332, 82, 1.00, 4, 'efectivo', 2),
(333, 82, 10.00, 3, 'efectivo', 5),
(334, 82, 20.00, 2, 'efectivo', 6),
(335, 82, 50.00, 6, 'efectivo', 7),
(336, 82, 100.00, 1, 'efectivo', 8),
(337, 82, 200.00, 3, 'efectivo', 9),
(338, 82, 500.00, 1, 'efectivo', 10),
(339, 82, 347.17, 1, 'boucher', NULL);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `entradas_detalle`
--

CREATE TABLE `entradas_detalle` (
  `id` int(11) NOT NULL,
  `entrada_id` int(11) DEFAULT NULL,
  `insumo_id` int(11) NOT NULL,
  `cantidad` int(11) DEFAULT NULL,
  `precio_unitario` decimal(10,2) DEFAULT NULL,
  `subtotal` decimal(10,2) GENERATED ALWAYS AS (`cantidad` * `precio_unitario`) STORED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `entradas_insumo`
--

CREATE TABLE `entradas_insumo` (
  `id` int(11) NOT NULL,
  `proveedor_id` int(11) DEFAULT NULL,
  `usuario_id` int(11) DEFAULT NULL,
  `fecha` datetime DEFAULT current_timestamp(),
  `total` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `facturas`
--

CREATE TABLE `facturas` (
  `id` int(11) NOT NULL,
  `ticket_id` int(11) NOT NULL,
  `cliente_id` int(11) NOT NULL,
  `folio` varchar(50) DEFAULT NULL,
  `uuid` varchar(64) DEFAULT NULL,
  `subtotal` decimal(10,2) NOT NULL DEFAULT 0.00,
  `impuestos` decimal(10,2) NOT NULL DEFAULT 0.00,
  `total` decimal(10,2) NOT NULL DEFAULT 0.00,
  `fecha_emision` datetime DEFAULT current_timestamp(),
  `estado` enum('generada','cancelada') DEFAULT 'generada',
  `notas` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `factura_detalles`
--

CREATE TABLE `factura_detalles` (
  `id` int(11) NOT NULL,
  `factura_id` int(11) NOT NULL,
  `ticket_detalle_id` int(11) DEFAULT NULL,
  `producto_id` int(11) DEFAULT NULL,
  `descripcion` varchar(255) DEFAULT NULL,
  `cantidad` int(11) NOT NULL DEFAULT 1,
  `precio_unitario` decimal(10,2) NOT NULL DEFAULT 0.00,
  `importe` decimal(10,2) NOT NULL DEFAULT 0.00
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `factura_tickets`
--

CREATE TABLE `factura_tickets` (
  `factura_id` int(11) NOT NULL,
  `ticket_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf16le COLLATE=utf16le_bin;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `fondo`
--

CREATE TABLE `fondo` (
  `usuario_id` int(11) NOT NULL,
  `monto` decimal(10,2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `fondo`
--

INSERT INTO `fondo` (`usuario_id`, `monto`) VALUES
(1, 1000.00);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `horarios`
--

CREATE TABLE `horarios` (
  `id` int(11) NOT NULL,
  `dia_semana` varchar(15) NOT NULL,
  `hora_inicio` time NOT NULL,
  `hora_fin` time NOT NULL,
  `serie_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `horarios`
--

INSERT INTO `horarios` (`id`, `dia_semana`, `hora_inicio`, `hora_fin`, `serie_id`) VALUES
(1, 'Sabado', '00:00:00', '23:59:59', 2),
(2, 'Lunes', '00:00:00', '23:59:00', 2),
(3, 'Viernes', '00:00:00', '23:59:00', 2),
(4, 'martes', '00:00:00', '23:59:59', 2),
(5, 'miercoles', '00:00:00', '23:59:59', 2),
(6, 'jueves', '00:00:00', '23:59:59', 2),
(7, 'domingo', '00:00:00', '23:59:59', 2);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `insumos`
--

CREATE TABLE `insumos` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) DEFAULT NULL,
  `unidad` varchar(20) DEFAULT NULL,
  `existencia` decimal(10,2) DEFAULT NULL,
  `tipo_control` enum('por_receta','unidad_completa','uso_general','no_controlado','desempaquetado') DEFAULT 'por_receta',
  `imagen` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `insumos`
--

INSERT INTO `insumos` (`id`, `nombre`, `unidad`, `existencia`, `tipo_control`, `imagen`) VALUES
(1, 'Arroz', 'gramos', 19480.00, 'por_receta', 'ins_68717301313ad.jpg'),
(2, 'Alga', 'piezas', 29972.00, 'por_receta', 'ins_6871716a72681.jpg'),
(3, 'Salmón fresco', 'gramos', 30000.00, 'por_receta', 'ins_6871777fa2c56.png'),
(4, 'Refresco en lata', 'piezas', 29999.00, 'unidad_completa', 'ins_6871731d075cb.webp'),
(7, 'Surimi', 'gramos', 30000.00, 'uso_general', 'ins_688a521dcd583.jpg'),
(8, 'Tocino', 'gramos', 29370.00, 'uso_general', 'ins_688a4dc84c002.jpg'),
(9, 'Pollo', 'gramos', 29720.00, 'desempaquetado', 'ins_688a4e4bd5999.jpg'),
(10, 'Camarón', 'gramos', 29515.00, 'desempaquetado', 'ins_688a4f5c873c6.jpg'),
(11, 'Queso Chihuahua', 'gramos', 29980.00, 'unidad_completa', 'ins_688a4feca9865.jpg'),
(12, 'Philadelphia', 'gramos', 28480.00, 'uso_general', 'ins_688a504f9cb40.jpg'),
(13, 'Arroz blanco', 'gramos', 30000.00, 'por_receta', 'ins_689f82d674c65.jpg'),
(14, 'Carne', 'gramos', 29460.00, 'uso_general', 'ins_688a528d1261a.jpg'),
(15, 'Queso Amarillo', 'piezas', 29992.00, 'uso_general', 'ins_688a53246c1c2.jpg'),
(16, 'Ajonjolí', 'gramos', 29989.00, 'uso_general', 'ins_689f824a23343.jpg'),
(17, 'Panko', 'gramos', 30000.00, 'por_receta', 'ins_688a53da64b5f.jpg'),
(18, 'Salsa tampico', 'mililitros', 30000.00, 'no_controlado', 'ins_688a54cf1872b.jpg'),
(19, 'Anguila', 'oz', 29992.00, 'por_receta', 'ins_689f828638aa9.jpg'),
(20, 'BBQ', 'oz', 29996.00, 'no_controlado', 'ins_688a557431fce.jpg'),
(21, 'Serrano', 'gramos', 29940.00, 'uso_general', 'ins_688a55c66f09d.jpg'),
(22, 'Chile Morrón', 'gramos', 30000.00, 'por_receta', 'ins_688a5616e8f25.jpg'),
(23, 'Kanikama', 'gramos', 29990.00, 'por_receta', 'ins_688a5669e24a8.jpg'),
(24, 'Aguacate', 'gramos', 28570.00, 'por_receta', 'ins_689f8254c2e71.jpg'),
(25, 'Dedos de queso', 'pieza', 30000.00, 'unidad_completa', 'ins_688a56fda3221.jpg'),
(26, 'Mango', 'gramos', 30000.00, 'por_receta', 'ins_688a573c762f4.jpg'),
(27, 'Tostadas', 'pieza', 30000.00, 'uso_general', 'ins_688a57a499b35.jpg'),
(28, 'Papa', 'gramos', 29750.00, 'por_receta', 'ins_688a580061ffd.jpg'),
(29, 'Cebolla Morada', 'gramos', 30000.00, 'por_receta', 'ins_688a5858752a0.jpg'),
(30, 'Salsa de soya', 'mililitros', 30000.00, 'no_controlado', 'ins_688a58cc6cb6c.jpg'),
(31, 'Naranja', 'gramos', 30000.00, 'por_receta', 'ins_688a590bca275.jpg'),
(32, 'Chile Caribe', 'gramos', 29985.00, 'por_receta', 'ins_688a59836c32e.jpg'),
(33, 'Pulpo', 'gramos', 29830.00, 'por_receta', 'ins_688a59c9a1d0b.jpg'),
(34, 'Zanahoria', 'gramos', 30000.00, 'por_receta', 'ins_688a5a0a3a959.jpg'),
(35, 'Apio', 'gramos', 30000.00, 'por_receta', 'ins_688a5a52af990.jpg'),
(36, 'Pepino', 'gramos', 28225.00, 'uso_general', 'ins_688a5aa0cbaf5.jpg'),
(37, 'Masago', 'gramos', 30000.00, 'por_receta', 'ins_688a5b3f0dca6.jpg'),
(38, 'Nuez de la india', 'gramos', 30000.00, 'por_receta', 'ins_688a5be531e11.jpg'),
(39, 'Cátsup', 'mililitros', 30000.00, 'por_receta', 'ins_688a5c657eb83.jpg'),
(40, 'Atún fresco', 'gramos', 30000.00, 'por_receta', 'ins_688a5ce18adc5.jpg'),
(41, 'Callo almeja', 'gramos', 30000.00, 'por_receta', 'ins_688a5d28de8a5.jpg'),
(42, 'Calabacin', 'gramos', 30000.00, 'por_receta', 'ins_688a5d6b2bca1.jpg'),
(43, 'Fideo chino transparente', 'gramos', 30000.00, 'por_receta', 'ins_688a5dd3b406d.jpg'),
(44, 'Brócoli', 'gramos', 30000.00, 'por_receta', 'ins_688a5e2736870.jpg'),
(45, 'Chile de árbol', 'gramos', 29960.00, 'por_receta', 'ins_688a5e6f08ccd.jpg'),
(46, 'Pasta udon', 'gramos', 29970.00, 'por_receta', 'ins_688a5eb627f38.jpg'),
(47, 'Huevo', 'pieza', 30000.00, 'por_receta', 'ins_688a5ef9b575e.jpg'),
(48, 'Cerdo', 'gramos', 29940.00, 'por_receta', 'ins_688a5f3915f5e.jpg'),
(49, 'Masa para gyozas', 'pieza', 30000.00, 'por_receta', 'ins_688a5fae2e7f1.jpg'),
(50, 'Naruto', 'gramos', 30000.00, 'por_receta', 'ins_688a5ff57f62d.jpg'),
(51, 'Atún ahumado', 'gramos', 30000.00, 'por_receta', 'ins_68adcd62c5a19.jpg'),
(52, 'Cacahuate con salsa (salado)', 'gramos', 30000.00, 'por_receta', 'ins_68adcf253bd1d.jpg'),
(53, 'Calabaza', 'gramos', 30000.00, 'por_receta', 'ins_68add0ff781fb.jpg'),
(54, 'Camarón gigante para pelar', 'pieza', 30000.00, 'por_receta', 'ins_68add3264c465.jpg'),
(55, 'Cebolla', 'gramos', 30000.00, 'por_receta', 'ins_68add38beff59.jpg'),
(56, 'Chile en polvo', 'gramos', 29995.00, 'por_receta', 'ins_68add4a750a0e.jpg'),
(57, 'Coliflor', 'gramos', 30000.00, 'por_receta', 'ins_68add5291130e.jpg'),
(59, 'Dedos de surimi', 'pieza', 30000.00, 'unidad_completa', 'ins_68add5c575fbb.jpg'),
(60, 'Fideos', 'gramos', 30000.00, 'por_receta', 'ins_68add629d094b.jpg'),
(61, 'Fondo de res', 'mililitros', 29880.00, 'no_controlado', 'ins_68add68d317d5.jpg'),
(62, 'Gravy Naranja', 'oz', 30000.00, 'no_controlado', 'ins_68add7bb461b3.jpg'),
(63, 'Salsa Aguachil', 'oz', 29990.00, 'no_controlado', 'ins_68ae000034b31.jpg'),
(64, 'Julianas de zanahoria', 'gramos', 30000.00, 'por_receta', 'ins_68add82c9c245.jpg'),
(65, 'Limón', 'gramos', 30000.00, 'por_receta', 'ins_68add890ee640.jpg'),
(66, 'Queso Mix', 'gramos', 28480.00, 'uso_general', 'ins_68ade1625f489.jpg'),
(67, 'Morrón', 'gramos', 30000.00, 'por_receta', 'ins_68addcbc6d15a.jpg'),
(69, 'Pasta chukasoba', 'gramos', 30000.00, 'por_receta', 'ins_68addd277fde6.jpg'),
(70, 'Pasta frita', 'gramos', 30000.00, 'por_receta', 'ins_68addd91a005e.jpg'),
(71, 'Queso crema', 'gramos', 30000.00, 'uso_general', 'ins_68ade11cdadcb.jpg'),
(72, 'Refresco embotellado', 'pieza', 29986.00, 'unidad_completa', 'ins_68adfdd53f04e.jpg'),
(73, 'res', 'gramos', 30000.00, 'uso_general', 'ins_68adfe2e49580.jpg'),
(74, 'Rodajas de naranja', 'gramos', 30000.00, 'por_receta', 'ins_68adfeccd68d8.jpg'),
(75, 'Salmón', 'gramos', 30000.00, 'por_receta', 'ins_68adffa2a2db0.jpg'),
(76, 'Salsa de anguila', 'mililitros', 30000.00, 'no_controlado', 'ins_68ae005f1b3cd.jpg'),
(77, 'Salsa teriyaki (dulce)', 'mililitros', 30000.00, 'no_controlado', 'ins_68ae00c53121a.jpg'),
(78, 'Salsas orientales', 'mililitros', 29980.00, 'no_controlado', 'ins_68ae01341e7b1.jpg'),
(79, 'Shisimi', 'gramos', 30000.00, 'uso_general', 'ins_68ae018d22a63.jpg'),
(80, 'Siracha', 'mililitros', 29925.00, 'no_controlado', 'ins_68ae03413da26.jpg'),
(81, 'Tampico', 'mililitros', 29890.00, 'uso_general', 'ins_68ae03f65bd71.jpg'),
(82, 'Tortilla de harina', 'pieza', 30000.00, 'unidad_completa', 'ins_68ae04b46d24a.jpg'),
(83, 'Tostada', 'pieza', 30000.00, 'unidad_completa', 'ins_68ae05924a02a.jpg'),
(85, 'Yakimeshi mini', 'gramos', 30000.00, 'por_receta', 'ins_68ae061b1175b.jpg'),
(86, 'Sal con Ajo', 'pieza', 30000.00, 'por_receta', 'ins_68adff6dbf111.jpg'),
(87, 'Aderezo Chipotle', 'mililitros', 28945.00, 'por_receta', 'ins_68adcabeb1ee9.jpg'),
(88, 'Mezcla de Horneado', 'gramos', 29920.00, 'por_receta', 'ins_68addaa3e53f7.jpg'),
(89, 'Aderezo', 'gramos', 29960.00, 'uso_general', 'ins_68adcc0771a3c.jpg'),
(90, 'Camarón Empanizado', 'gramos', 28245.00, 'por_receta', 'ins_68add1de1aa0e.jpg'),
(91, 'Pollo Empanizado', 'gramos', 29850.00, 'por_receta', 'ins_68adde81c6be3.jpg'),
(92, 'Cebollín', 'gramos', 29970.00, 'por_receta', 'ins_68add3e38d04b.jpg'),
(93, 'Aderezo Cebolla Dul.', 'oz', 30000.00, 'uso_general', 'ins_68adcb8fa562e.jpg'),
(94, 'Camaron Enchiloso', 'gramos', 29840.00, 'por_receta', 'ins_68add2db69e2e.jpg'),
(95, 'Pastel chocoflan', 'pieza', 30000.00, 'unidad_completa', 'ins_68adddfa22fe2.jpg'),
(96, 'Pay de queso', 'pieza', 30000.00, 'unidad_completa', 'ins_68adde4fa8275.jpg'),
(97, 'Helado tempura', 'pieza', 30000.00, 'unidad_completa', 'ins_68add7e53c6fe.jpg'),
(98, 'Postre especial', 'pieza', 30000.00, 'unidad_completa', 'ins_68addee98fdf0.jpg'),
(99, 'Búfalo', 'mililitros', 29990.00, 'no_controlado', 'ins_68adce63dd347.jpg'),
(101, 'Corona 1/2', 'pieza', 29999.00, 'unidad_completa', 'ins_68add55a1e3b7.jpg'),
(102, 'Golden Light 1/2', 'pieza', 30000.00, 'unidad_completa', 'ins_68add76481f22.jpg'),
(103, 'Negra Modelo', 'pieza', 30000.00, 'unidad_completa', 'ins_68addc59c2ea9.jpg'),
(104, 'Modelo Especial', 'pieza', 29996.00, 'unidad_completa', 'ins_68addb9d59000.jpg'),
(105, 'Bud Light', 'pieza', 29997.00, 'unidad_completa', 'ins_68adcdf3295e8.jpg'),
(106, 'Stella Artois', 'pieza', 30000.00, 'unidad_completa', 'ins_68ae0397afb2f.jpg'),
(107, 'Ultra 1/2', 'pieza', 30000.00, 'unidad_completa', 'ins_68ae05466a8e2.jpg'),
(108, 'Michelob 1/2', 'pieza', 30000.00, 'unidad_completa', 'ins_68addb2d00c85.jpg'),
(109, 'Alitas de pollo', 'gramos', 29200.00, 'unidad_completa', 'ins_68adccf5a1147.jpg'),
(110, 'Ranch', 'mililitros', 30000.00, 'no_controlado', 'ins_68adfcddef7e3.jpg'),
(111, 'Buffalo', 'gramos', 30000.00, 'no_controlado', ''),
(112, 'Chichimi', 'gramos', 30000.00, 'no_controlado', 'ins_68add45bdb306.jpg'),
(113, 'Calpico', 'pieza', 29820.00, 'unidad_completa', 'ins_68add19570673.jpg'),
(114, 'Vaina de soja', 'gramos', 30000.00, 'uso_general', 'ins_68ae05de869d1.jpg'),
(115, 'Boneless', 'gramos', 29750.00, 'por_receta', 'ins_68adcdbb6b5b4.jpg'),
(116, 'Agua members', 'pieza', 30000.00, 'unidad_completa', 'ins_68adcc5feaee1.jpg'),
(117, 'Agua mineral', 'pieza', 29400.00, 'unidad_completa', 'ins_68adcca85ae2c.jpg'),
(118, 'Cilantro', 'gramos', 29990.00, 'por_receta', 'ins_68add4edab118.jpg'),
(119, 'Té de jazmin', 'mililitros', 30000.00, 'por_receta', 'ins_68ae0474dfc36.jpg'),
(120, 'bolsa camiseta 35x60', 'kilo', 0.00, 'unidad_completa', ''),
(121, 'bolsa camiseta 25x50', 'kilo', 0.00, 'unidad_completa', ''),
(122, 'bolsa camiseta 25x40', 'kilo', 0.00, 'unidad_completa', ''),
(123, 'bolsa poliseda 15x25', 'rollo', 0.00, 'unidad_completa', ''),
(124, 'bolsa rollo 20x30', 'rollo', 0.00, 'unidad_completa', ''),
(125, 'bowls cpp1911-3', 'pieza', 0.00, 'unidad_completa', ''),
(126, 'bowls cpp20', 'pieza', 0.00, 'unidad_completa', ''),
(127, 'bowls cpp1911-3 tapa', 'pieza', 0.00, 'unidad_completa', ''),
(128, 'bowls cpp20 tapa', 'pieza', 0.00, 'unidad_completa', ''),
(129, 'baso termico 1l', 'piza', 0.00, 'unidad_completa', ''),
(130, 'bisagra 22x22', 'pieza', 0.00, 'unidad_completa', ''),
(131, 'servilleta', 'paquete', 0.00, 'unidad_completa', ''),
(132, 'Papel aluminio 400', 'pieza', 0.00, 'unidad_completa', ''),
(133, 'Vitafilim 14', 'rollo', 0.00, 'unidad_completa', ''),
(134, 'guante vinil', 'caja', 0.00, 'unidad_completa', ''),
(135, 'Popote 26cm', 'pieza', 0.00, 'unidad_completa', ''),
(136, 'Bolsa papel x 100pz', 'paquete', 0.00, 'unidad_completa', ''),
(137, 'rollo impresora mediano', 'rollo', 0.00, 'unidad_completa', ''),
(138, 'rollo impresora grande', 'rollo', 0.00, 'unidad_completa', ''),
(139, 'tenedor fantasy mediano 25pz', 'paquete', 0.00, 'unidad_completa', ''),
(140, 'Bolsa basura 90x120 negra', 'bulto', 0.00, 'unidad_completa', ''),
(141, 'Ts2', 'tira', 0.00, 'unidad_completa', ''),
(142, 'Ts1', 'tira', 0.00, 'unidad_completa', ''),
(143, 'TS200', 'tira', 0.00, 'unidad_completa', ''),
(144, 'S100', 'tira', 0.00, 'unidad_completa', ''),
(145, 'Pet 1l c/tapa', 'bulto', 0.00, 'unidad_completa', ''),
(146, 'Pet 1/2l c/tapa', 'pieza', 0.00, 'unidad_completa', ''),
(147, 'Cuchara mediana fantasy 50pz', 'paquete', 0.00, 'unidad_completa', ''),
(148, 'Charola 8x8', 'pieza', 0.00, 'unidad_completa', ''),
(149, 'Charola 6x6', 'pieza', 0.00, 'unidad_completa', ''),
(150, 'Charola 8x8 negra', 'pieza', 0.00, 'unidad_completa', ''),
(151, 'Charola 6x6 negra', 'pieza', 0.00, 'unidad_completa', ''),
(152, 'Polipapel', 'kilo', 0.00, 'unidad_completa', ''),
(153, 'Charola pastelera', 'pieza', 0.00, 'unidad_completa', ''),
(154, 'Papel secante', 'pieza', 0.00, 'unidad_completa', ''),
(155, 'Papel rollo higienico', 'pieza', 0.00, 'unidad_completa', ''),
(156, 'Fabuloso 20l', 'bidon', 0.00, 'unidad_completa', ''),
(157, 'Desengrasante 20l', 'bidon', 0.00, 'unidad_completa', ''),
(158, 'Cloro 20l', 'bidon', 0.00, 'unidad_completa', ''),
(159, 'Iorizante 20l', 'bidon', 0.00, 'unidad_completa', ''),
(160, 'Windex 20l', 'bidon', 0.00, 'unidad_completa', ''),
(161, 'quitacochambre 1l', 'litro', 0.00, 'unidad_completa', ''),
(162, 'Fibra metal', 'pieza', 0.00, 'unidad_completa', ''),
(163, 'Esponja', 'pieza', 0.00, 'unidad_completa', ''),
(164, 'Escoba', 'pieza', 0.00, 'unidad_completa', ''),
(165, 'Recogedor', 'pieza', 0.00, 'unidad_completa', ''),
(166, 'Trapeador', 'pieza', 0.00, 'unidad_completa', ''),
(167, 'Cubeta 16l', 'pieza', 0.00, 'unidad_completa', ''),
(168, 'Sanitas', 'paquete', 0.00, 'unidad_completa', ''),
(169, 'Jabon polvo 9k', 'bulto', 0.00, 'unidad_completa', ''),
(170, 'Shampoo trastes 20l', 'bidon', 0.00, 'unidad_completa', ''),
(171, 'Jaladores', 'pieza', 0.00, 'unidad_completa', ''),
(172, 'Cofia', 'pieza', 0.00, 'unidad_completa', ''),
(173, 'Trapo', 'pieza', 0.00, 'unidad_completa', ''),
(174, 'Sambal', 'mililitros', 0.00, 'por_receta', ''),
(175, 'Lemon pepper', 'gramos', 0.00, 'por_receta', ''),
(176, 'Consomé', 'gramos', 0.00, 'por_receta', ''),
(177, 'Ejote', 'gramos', 0.00, 'por_receta', ''),
(178, 'Chili bean', 'gramos', 0.00, 'por_receta', ''),
(179, 'Ajinomoto', 'gramos', 0.00, 'por_receta', ''),
(180, 'Salsa Yakimeshi', 'mililitros', 0.00, 'no_controlado', ''),
(181, 'Papas a la francesa (porción kid)', 'porción', 0.00, 'unidad_completa', ''),
(182, 'Cacahuate', 'gramos', -40.00, 'por_receta', ''),
(183, 'Boneless (porción)', 'porción', 0.00, 'unidad_completa', ''),
(184, 'Liner', 'pieza', 0.00, 'unidad_completa', ''),
(185, 'Capeador General', 'mililitros', 0.00, 'no_controlado', ''),
(186, 'Ajo', 'gramos', 0.00, 'por_receta', ''),
(187, 'Jengibre', 'gramos', 0.00, 'por_receta', ''),
(188, 'Hoisin', 'gramos', 0.00, 'por_receta', ''),
(189, 'Col morada', 'gramos', 0.00, 'por_receta', ''),
(190, 'Champiñon', 'kilo', 0.00, 'uso_general', ''),
(191, 'Rabano', 'kilo', 0.00, 'por_receta', '');

--
-- Disparadores `insumos`
--
DELIMITER $$
CREATE TRIGGER `trg_update_insumo_existencia` AFTER UPDATE ON `insumos` FOR EACH ROW BEGIN
    IF NEW.existencia != OLD.existencia THEN
        CALL sp_recalcular_productos_por_insumo(NEW.id);
    END IF;
END
$$
DELIMITER ;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `logs_accion`
--

CREATE TABLE `logs_accion` (
  `id` int(11) NOT NULL,
  `usuario_id` int(11) DEFAULT NULL,
  `modulo` varchar(50) DEFAULT NULL,
  `accion` varchar(100) DEFAULT NULL,
  `fecha` datetime DEFAULT current_timestamp(),
  `referencia_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `logs_accion`
--

INSERT INTO `logs_accion` (`id`, `usuario_id`, `modulo`, `accion`, `fecha`, `referencia_id`) VALUES
(646, 1, 'corte_caja', 'Creación de corte', '2025-08-27 11:11:28', 58),
(647, 6, 'ventas', 'Alta de venta', '2025-08-27 11:12:37', 120),
(648, 2, 'ventas', 'Alta de venta', '2025-08-27 11:33:50', 121),
(649, 6, 'ventas', 'Alta de venta', '2025-08-27 11:58:47', 122),
(650, 5, 'ventas', 'Alta de venta', '2025-08-27 11:59:12', 123),
(651, NULL, 'cocina', 'Producto iniciado', '2025-08-27 13:48:48', 234),
(652, NULL, 'cocina', 'Producto iniciado', '2025-08-27 13:48:49', 235),
(653, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 13:48:50', 235),
(654, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 13:48:54', 234),
(655, NULL, 'cocina', 'Producto iniciado', '2025-08-27 13:49:33', 236),
(656, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 15:58:17', 236),
(657, 5, 'ventas', 'Alta de venta', '2025-08-27 16:09:28', 124),
(658, 5, 'ventas', 'Alta de venta', '2025-08-27 18:55:53', 125),
(659, NULL, 'cocina', 'Producto iniciado', '2025-08-27 18:56:01', 240),
(660, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 18:56:02', 240),
(661, 2, 'ventas', 'Alta de venta', '2025-08-27 18:57:17', 126),
(662, NULL, 'cocina', 'Producto iniciado', '2025-08-27 18:57:22', 241),
(663, NULL, 'cocina', 'Producto iniciado', '2025-08-27 18:57:23', 242),
(664, NULL, 'cocina', 'Producto iniciado', '2025-08-27 18:57:24', 243),
(665, NULL, 'cocina', 'Producto iniciado', '2025-08-27 18:57:24', 244),
(666, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 18:57:25', 241),
(667, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 18:57:26', 242),
(668, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 18:57:26', 243),
(669, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 18:57:27', 244),
(670, 1, 'corte_caja', 'Cierre de corte', '2025-08-27 19:01:33', 58),
(671, 1, 'corte_caja', 'Creación de corte', '2025-08-27 19:14:02', 59),
(672, 4, 'ventas', 'Alta de venta', '2025-08-27 19:14:36', 127),
(673, NULL, 'cocina', 'Producto iniciado', '2025-08-27 19:14:42', 245),
(674, NULL, 'cocina', 'Producto iniciado', '2025-08-27 19:14:43', 246),
(675, NULL, 'cocina', 'Producto iniciado', '2025-08-27 19:14:44', 247),
(676, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 19:14:44', 245),
(677, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 19:14:48', 246),
(678, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 19:14:49', 247),
(679, 1, 'corte_caja', 'Cierre de corte', '2025-08-27 19:17:23', 59),
(680, 1, 'corte_caja', 'Creación de corte', '2025-08-27 19:25:15', 60),
(681, 4, 'ventas', 'Alta de venta', '2025-08-27 19:25:29', 128),
(682, NULL, 'cocina', 'Producto iniciado', '2025-08-27 19:26:20', 248),
(683, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 19:26:21', 248),
(684, 1, 'corte_caja', 'Cierre de corte', '2025-08-27 19:27:33', 60),
(685, 1, 'corte_caja', 'Creación de corte', '2025-08-27 19:29:49', 61),
(686, 5, 'ventas', 'Alta de venta', '2025-08-27 19:30:40', 129),
(687, NULL, 'cocina', 'Producto iniciado', '2025-08-27 19:30:45', 249),
(688, NULL, 'cocina', 'Producto marcado como listo', '2025-08-27 19:30:45', 249),
(689, 5, 'ventas', 'Alta de venta', '2025-08-28 16:59:24', 130),
(690, NULL, 'cocina', 'Producto iniciado', '2025-08-28 16:59:43', 250),
(691, NULL, 'cocina', 'Producto marcado como listo', '2025-08-28 16:59:46', 250),
(692, 1, 'corte_caja', 'Cierre de corte', '2025-08-28 18:54:48', 61),
(693, 1, 'corte_caja', 'Creación de corte', '2025-08-29 10:03:01', 62),
(694, 36, 'ventas', 'Alta de venta', '2025-08-29 15:53:17', 132),
(695, 36, 'ventas', 'Alta de venta', '2025-08-29 16:04:24', 133),
(696, NULL, 'cocina', 'Producto iniciado', '2025-08-29 16:18:27', 251),
(697, NULL, 'cocina', 'Producto iniciado', '2025-08-29 16:18:28', 253),
(698, NULL, 'cocina', 'Producto iniciado', '2025-08-29 16:18:29', 255),
(699, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 16:18:30', 253),
(700, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 16:18:31', 251),
(701, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 16:18:32', 255),
(702, NULL, 'cocina', 'Producto iniciado', '2025-08-29 21:37:04', 257),
(703, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 21:37:05', 257),
(704, 6, 'ventas', 'Alta de venta', '2025-08-29 21:46:04', 134),
(705, NULL, 'cocina', 'Producto iniciado', '2025-08-29 21:46:12', 258),
(706, NULL, 'cocina', 'Producto iniciado', '2025-08-29 21:46:14', 259),
(707, NULL, 'cocina', 'Producto iniciado', '2025-08-29 21:46:14', 260),
(708, NULL, 'cocina', 'Producto iniciado', '2025-08-29 21:46:15', 261),
(709, NULL, 'cocina', 'Producto iniciado', '2025-08-29 21:46:19', 262),
(710, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 21:46:20', 258),
(711, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 21:46:21', 259),
(712, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 21:46:22', 260),
(713, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 21:46:23', 261),
(714, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 21:46:24', 262),
(715, 5, 'ventas', 'Alta de venta', '2025-08-29 21:58:00', 135),
(716, NULL, 'cocina', 'Producto iniciado', '2025-08-29 21:58:07', 263),
(717, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 21:58:08', 263),
(718, 17, 'ventas', 'Alta de venta', '2025-08-29 22:03:04', 136),
(719, NULL, 'cocina', 'Producto iniciado', '2025-08-29 22:03:10', 264),
(720, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 22:03:12', 264),
(721, 5, 'ventas', 'Alta de venta', '2025-08-29 22:26:06', 137),
(722, NULL, 'cocina', 'Producto iniciado', '2025-08-29 22:26:10', 265),
(723, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 22:26:11', 265),
(724, 1, 'corte_caja', 'Cierre de corte', '2025-08-29 22:30:17', 62),
(725, 1, 'corte_caja', 'Creación de corte', '2025-08-29 22:40:21', 63),
(726, 6, 'ventas', 'Alta de venta', '2025-08-29 22:40:49', 138),
(727, NULL, 'cocina', 'Producto iniciado', '2025-08-29 22:40:53', 266),
(728, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 22:40:54', 266),
(729, NULL, 'cocina', 'Producto iniciado', '2025-08-29 22:40:56', 267),
(730, NULL, 'cocina', 'Producto marcado como listo', '2025-08-29 22:40:57', 267),
(731, 1, 'corte_caja', 'Cierre de corte', '2025-08-29 23:08:55', 63),
(732, 1, 'corte_caja', 'Creación de corte', '2025-08-30 11:32:10', 64),
(733, 5, 'ventas', 'Alta de venta', '2025-08-30 11:32:31', 139),
(734, NULL, 'cocina', 'Producto iniciado', '2025-08-30 11:32:45', 268),
(735, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 11:32:46', 268),
(736, 1, 'corte_caja', 'Creación de corte', '2025-08-30 11:49:11', 65),
(737, 17, 'ventas', 'Alta de venta', '2025-08-30 11:52:16', 140),
(738, NULL, 'cocina', 'Producto iniciado', '2025-08-30 11:52:22', 269),
(739, NULL, 'cocina', 'Producto iniciado', '2025-08-30 11:52:23', 270),
(740, NULL, 'cocina', 'Producto iniciado', '2025-08-30 11:52:26', 271),
(741, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 11:52:28', 269),
(742, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 11:52:30', 270),
(743, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 11:52:31', 271),
(744, 1, 'corte_caja', 'Cierre de corte', '2025-08-30 11:57:24', 65),
(745, 1, 'corte_caja', 'Creación de corte', '2025-08-30 12:39:13', 66),
(746, 2, 'ventas', 'Alta de venta', '2025-08-30 12:39:37', 141),
(747, NULL, 'cocina', 'Producto iniciado', '2025-08-30 12:39:42', 272),
(748, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 12:39:43', 272),
(749, 6, 'ventas', 'Alta de venta', '2025-08-30 12:47:45', 142),
(750, NULL, 'cocina', 'Producto iniciado', '2025-08-30 12:47:54', 273),
(751, NULL, 'cocina', 'Producto iniciado', '2025-08-30 12:47:55', 274),
(752, NULL, 'cocina', 'Producto iniciado', '2025-08-30 12:47:55', 275),
(753, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 12:47:56', 273),
(754, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 12:51:02', 274),
(755, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 12:51:49', 275),
(756, 2, 'ventas', 'Alta de venta', '2025-08-30 13:13:09', 143),
(757, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:13:12', 276),
(758, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:13:13', 276),
(759, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:13:14', 277),
(760, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:13:15', 278),
(761, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:13:16', 277),
(762, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:13:17', 278),
(763, 1, 'corte_caja', 'Cierre de corte', '2025-08-30 13:19:16', 66),
(764, 1, 'corte_caja', 'Creación de corte', '2025-08-30 13:22:33', 67),
(765, 5, 'ventas', 'Alta de venta', '2025-08-30 13:22:58', 144),
(766, 2, 'ventas', 'Alta de venta', '2025-08-30 13:23:24', 145),
(767, 35, 'ventas', 'Alta de venta', '2025-08-30 13:25:18', 146),
(768, 6, 'ventas', 'Alta de venta', '2025-08-30 13:25:45', 147),
(769, 17, 'ventas', 'Alta de venta', '2025-08-30 13:26:02', 148),
(770, 5, 'ventas', 'Alta de venta', '2025-08-30 13:26:23', 149),
(771, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:27', 279),
(772, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:28', 280),
(773, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:28', 281),
(774, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:29', 282),
(775, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:30', 279),
(776, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:30', 280),
(777, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:32', 283),
(778, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:33', 286),
(779, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:34', 287),
(780, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:35', 285),
(781, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:26:36', 288),
(782, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:36', 282),
(783, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:37', 281),
(784, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:38', 283),
(785, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:39', 287),
(786, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:43', 285),
(787, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:44', 286),
(788, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:26:46', 288),
(789, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:30:45', 289),
(790, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:30:45', 289),
(791, 4, 'ventas', 'Alta de venta', '2025-08-30 13:32:06', 150),
(792, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:32:09', 290),
(793, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:32:10', 291),
(794, NULL, 'cocina', 'Producto iniciado', '2025-08-30 13:32:11', 292),
(795, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:32:11', 290),
(796, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:32:13', 291),
(797, NULL, 'cocina', 'Producto marcado como listo', '2025-08-30 13:32:15', 292),
(798, 1, 'corte_caja', 'Cierre de corte', '2025-08-30 13:34:26', 67),
(799, 1, 'corte_caja', 'Creación de corte', '2025-08-30 13:57:46', 68),
(800, 5, 'ventas', 'Alta de venta', '2025-08-30 13:58:06', 151),
(801, 5, 'ventas', 'Alta de venta', '2025-08-30 13:58:46', 152),
(802, 5, 'ventas', 'Alta de venta', '2025-08-30 13:58:55', 153),
(803, 1, 'corte_caja', 'Cierre de corte', '2025-08-30 13:59:36', 68),
(804, 1, 'corte_caja', 'Creación de corte', '2025-09-02 11:04:28', 69),
(805, 35, 'ventas', 'Alta de venta', '2025-09-02 11:05:55', 154),
(806, NULL, 'cocina', 'Producto iniciado', '2025-09-02 11:06:17', 296),
(807, NULL, 'cocina', 'Producto iniciado', '2025-09-02 11:06:18', 297),
(808, NULL, 'cocina', 'Producto marcado como listo', '2025-09-02 11:06:19', 296),
(809, NULL, 'cocina', 'Producto marcado como listo', '2025-09-02 11:06:20', 297),
(810, 1, 'corte_caja', 'Cierre de corte', '2025-09-02 11:07:30', 69),
(811, 1, 'corte_caja', 'Creación de corte', '2025-09-04 21:41:17', 70),
(812, 5, 'ventas', 'Alta de venta', '2025-09-04 21:54:00', 155),
(813, NULL, 'cocina', 'Producto iniciado', '2025-09-04 21:56:24', 299),
(814, NULL, 'cocina', 'Producto marcado como listo', '2025-09-04 21:56:25', 299),
(815, 1, 'corte_caja', 'Cierre de corte', '2025-09-07 02:22:54', 70),
(816, 1, 'corte_caja', 'Creación de corte', '2025-09-07 02:23:16', 71),
(817, 6, 'ventas', 'Alta de venta', '2025-09-07 02:23:33', 156),
(818, NULL, 'cocina', 'Producto iniciado', '2025-09-07 02:23:40', 300),
(819, NULL, 'cocina', 'Producto marcado como listo', '2025-09-07 02:23:42', 300),
(820, 6, 'ventas', 'Alta de venta', '2025-09-07 02:38:49', 157),
(821, NULL, 'cocina', 'Producto iniciado', '2025-09-07 02:39:07', 301),
(822, NULL, 'cocina', 'Producto marcado como listo', '2025-09-07 02:39:08', 301),
(823, NULL, 'cocina', 'Producto iniciado', '2025-09-07 02:46:26', 302),
(824, NULL, 'cocina', 'Producto marcado como listo', '2025-09-07 02:46:27', 302),
(825, 1, 'corte_caja', 'Cierre de corte', '2025-09-07 03:10:27', 71),
(826, 1, 'corte_caja', 'Creación de corte', '2025-09-07 18:41:13', 72),
(827, 2, 'ventas', 'Alta de venta', '2025-09-07 18:44:09', 158),
(828, NULL, 'cocina', 'Producto iniciado', '2025-09-07 18:44:15', 303),
(829, NULL, 'cocina', 'Producto marcado como listo', '2025-09-07 18:44:17', 303),
(830, NULL, 'cocina', 'Producto iniciado', '2025-09-07 18:44:17', 304),
(831, NULL, 'cocina', 'Producto marcado como listo', '2025-09-07 18:44:18', 304),
(832, NULL, 'cocina', 'Producto iniciado', '2025-09-07 23:56:08', 305),
(833, NULL, 'cocina', 'Producto marcado como listo', '2025-09-07 23:56:09', 305),
(834, 6, 'ventas', 'Alta de venta', '2025-09-08 00:02:46', 159),
(835, NULL, 'cocina', 'Producto iniciado', '2025-09-08 00:02:54', 306),
(836, NULL, 'cocina', 'Producto marcado como listo', '2025-09-08 00:02:55', 306),
(837, NULL, 'cocina', 'Producto iniciado', '2025-09-08 00:02:56', 307),
(838, NULL, 'cocina', 'Producto marcado como listo', '2025-09-08 00:02:57', 307),
(839, NULL, 'cocina', 'Producto iniciado', '2025-09-08 00:02:58', 308),
(840, NULL, 'cocina', 'Producto marcado como listo', '2025-09-08 00:02:59', 308),
(841, 4, 'ventas', 'Alta de venta', '2025-09-08 22:44:20', 160),
(842, NULL, 'cocina', 'Producto iniciado', '2025-09-08 22:44:27', 309),
(843, NULL, 'cocina', 'Producto iniciado', '2025-09-08 22:44:29', 310),
(844, NULL, 'cocina', 'Producto marcado como listo', '2025-09-08 22:44:29', 309),
(845, NULL, 'cocina', 'Producto marcado como listo', '2025-09-08 22:44:31', 310),
(846, 1, 'corte_caja', 'Cierre de corte', '2025-09-09 12:22:13', 72),
(847, 1, 'corte_caja', 'Creación de corte', '2025-09-09 12:25:54', 73),
(848, 6, 'ventas', 'Alta de venta', '2025-09-09 12:26:10', 161),
(849, NULL, 'cocina', 'Producto iniciado', '2025-09-09 12:26:14', 311),
(850, NULL, 'cocina', 'Producto marcado como listo', '2025-09-09 12:26:15', 311),
(851, NULL, 'cocina', 'Producto iniciado', '2025-09-09 12:26:16', 312),
(852, NULL, 'cocina', 'Producto marcado como listo', '2025-09-09 12:26:17', 312),
(853, 1, 'corte_caja', 'Cierre de corte', '2025-09-09 12:46:02', 73),
(854, 1, 'corte_caja', 'Creación de corte', '2025-09-09 12:51:41', 74),
(855, 6, 'ventas', 'Alta de venta', '2025-09-09 12:51:56', 162),
(856, NULL, 'cocina', 'Producto iniciado', '2025-09-09 12:52:04', 313),
(857, NULL, 'cocina', 'Producto marcado como listo', '2025-09-09 12:52:04', 313),
(858, NULL, 'cocina', 'Producto iniciado', '2025-09-09 12:52:05', 314),
(859, NULL, 'cocina', 'Producto marcado como listo', '2025-09-09 12:52:06', 314),
(860, 4, 'ventas', 'Alta de venta', '2025-09-11 22:26:28', 163),
(861, 1, 'ventas', 'Alta de venta', '2025-09-12 17:27:22', 164),
(862, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:38:25', 315),
(863, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:38:25', 316),
(864, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:38:26', 317),
(865, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:38:27', 318),
(866, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:38:29', 319),
(867, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:38:30', 320),
(868, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:38:30', 321),
(869, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:38:31', 315),
(870, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:38:32', 316),
(871, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:38:34', 317),
(872, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:38:35', 318),
(873, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:38:36', 319),
(874, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:38:36', 320),
(875, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:38:40', 321),
(876, NULL, 'cocina', 'Producto iniciado', '2025-09-12 17:39:44', 322),
(877, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 17:39:45', 322),
(878, 1, 'ventas', 'Alta de venta', '2025-09-12 18:05:07', 165),
(879, 1, 'ventas', 'Alta de venta', '2025-09-12 18:14:43', 166),
(880, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:02', 323),
(881, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:03', 324),
(882, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:04', 325),
(883, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:05', 326),
(884, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:05', 327),
(885, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:06', 328),
(886, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:07', 329),
(887, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:07', 330),
(888, NULL, 'cocina', 'Producto iniciado', '2025-09-12 18:15:08', 331),
(889, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:09', 323),
(890, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:09', 324),
(891, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:10', 325),
(892, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:10', 326),
(893, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:11', 327),
(894, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:12', 328),
(895, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:13', 329),
(896, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:13', 330),
(897, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 18:15:14', 331),
(898, 1, 'corte_caja', 'Cierre de corte', '2025-09-12 18:18:43', 74),
(899, NULL, 'cocina', 'Producto iniciado', '2025-09-12 21:34:41', 332),
(900, NULL, 'cocina', 'Producto iniciado', '2025-09-12 21:34:42', 333),
(901, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 21:34:43', 332),
(902, NULL, 'cocina', 'Producto marcado como listo', '2025-09-12 21:34:44', 333),
(903, NULL, 'cocina', 'Producto iniciado', '2025-09-13 12:12:31', 336),
(904, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 12:12:32', 336),
(905, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:16', 337),
(906, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:17', 338),
(907, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:18', 339),
(908, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:18', 340),
(909, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:19', 341),
(910, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:20', 342),
(911, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:21', 343),
(912, NULL, 'cocina', 'Producto iniciado', '2025-09-13 14:04:22', 344),
(913, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:19', 337),
(914, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:20', 338),
(915, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:22', 339),
(916, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:24', 340),
(917, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:25', 341),
(918, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:25', 342),
(919, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:26', 343),
(920, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 14:05:26', 344),
(921, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:05:51', 1347),
(922, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:05:52', 1348),
(923, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:05:53', 1349),
(924, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:05:54', 1350),
(925, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:13:02', 1347),
(926, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:13:03', 1348),
(927, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:13:03', 1349),
(928, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:13:04', 1350),
(929, 1, 'corte_caja', 'Creación de corte', '2025-09-13 16:25:08', 75),
(930, 1, 'corte_caja', 'Creación de corte', '2025-09-13 16:26:15', 76),
(931, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:29:00', 1352),
(932, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:29:01', 1352),
(933, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:44:40', 1354),
(934, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:44:40', 1355),
(935, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:44:41', 1354),
(936, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:44:42', 1355),
(937, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:44:44', 1356),
(938, NULL, 'cocina', 'Producto iniciado', '2025-09-13 16:44:44', 1357),
(939, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:44:45', 1356),
(940, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 16:44:46', 1357),
(941, 1, 'corte_caja', 'Cierre de corte', '2025-09-13 17:03:54', 76),
(942, 1, 'corte_caja', 'Creación de corte', '2025-09-13 17:14:00', 77),
(943, 1, 'corte_caja', 'Cierre de corte', '2025-09-13 17:14:07', 77),
(944, 1, 'corte_caja', 'Creación de corte', '2025-09-13 17:14:20', 78),
(945, 1, 'corte_caja', 'Cierre de corte', '2025-09-13 17:14:35', 78),
(946, 1, 'corte_caja', 'Creación de corte', '2025-09-13 17:26:15', 79),
(947, 5, 'ventas', 'Alta de venta', '2025-09-13 17:26:48', 343),
(948, 1, 'corte_caja', 'Cierre de corte', '2025-09-13 17:28:46', 79),
(949, 1, 'corte_caja', 'Creación de corte', '2025-09-13 17:33:10', 80),
(950, 1, 'corte_caja', 'Cierre de corte', '2025-09-13 17:33:18', 80),
(951, 1, 'corte_caja', 'Creación de corte', '2025-09-13 17:36:58', 81),
(952, 1, 'corte_caja', 'Cierre de corte', '2025-09-13 17:37:15', 81),
(953, NULL, 'cocina', 'Producto iniciado', '2025-09-13 17:50:23', 1359),
(954, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 17:50:23', 1359),
(955, NULL, 'cocina', 'Producto iniciado', '2025-09-13 17:50:24', 1360),
(956, NULL, 'cocina', 'Producto iniciado', '2025-09-13 17:50:25', 1361),
(957, NULL, 'cocina', 'Producto iniciado', '2025-09-13 17:50:26', 1362),
(958, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 17:50:26', 1360),
(959, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 17:50:27', 1361),
(960, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 17:50:28', 1362),
(961, 1, 'corte_caja', 'Creación de corte', '2025-09-13 19:45:56', 82),
(962, 35, 'ventas', 'Alta de venta', '2025-09-13 20:01:03', 345),
(963, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:23:43', 1363),
(964, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:23:44', 1365),
(965, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:23:45', 1364),
(966, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:23:47', 1367),
(967, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:23:48', 1366),
(968, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:23:48', 1363),
(969, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:23:49', 1365),
(970, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:23:50', 1367),
(971, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:23:51', 1369),
(972, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:24:29', 1364),
(973, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:24:30', 1366),
(974, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:24:31', 1369),
(975, 1, 'corte_caja', 'Cierre de corte', '2025-09-13 20:25:38', 82),
(976, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:53:01', 1370),
(977, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:53:02', 1370),
(978, NULL, 'cocina', 'Producto iniciado', '2025-09-13 20:53:03', 1371),
(979, NULL, 'cocina', 'Producto marcado como listo', '2025-09-13 20:53:04', 1371);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `log_asignaciones_mesas`
--

CREATE TABLE `log_asignaciones_mesas` (
  `id` int(11) NOT NULL,
  `mesa_id` int(11) NOT NULL,
  `mesero_anterior_id` int(11) DEFAULT NULL,
  `mesero_nuevo_id` int(11) DEFAULT NULL,
  `fecha_cambio` datetime DEFAULT current_timestamp(),
  `usuario_que_asigna_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `log_cancelaciones`
--

CREATE TABLE `log_cancelaciones` (
  `id` int(11) NOT NULL,
  `tipo` enum('venta','detalle') NOT NULL,
  `venta_id` int(11) DEFAULT NULL,
  `venta_detalle_id` int(11) DEFAULT NULL,
  `usuario_id` int(11) DEFAULT NULL,
  `motivo` varchar(255) DEFAULT NULL,
  `total_anterior` decimal(10,2) DEFAULT NULL,
  `subtotal_detalle` decimal(10,2) DEFAULT NULL,
  `fecha` datetime NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `log_mesas`
--

CREATE TABLE `log_mesas` (
  `id` int(11) NOT NULL,
  `mesa_id` int(11) NOT NULL,
  `venta_id` int(11) DEFAULT NULL,
  `usuario_id` int(11) DEFAULT NULL,
  `fecha_inicio` datetime DEFAULT NULL,
  `fecha_fin` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `menu_dia`
--

CREATE TABLE `menu_dia` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) DEFAULT NULL,
  `precio` decimal(10,2) DEFAULT NULL,
  `fecha` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `menu_dia`
--

INSERT INTO `menu_dia` (`id`, `nombre`, `precio`, `fecha`) VALUES
(1, 'Enchiladas Verdes', 85.00, '2025-07-28'),
(2, 'Pozole', 95.00, '2025-07-28'),
(3, 'Tacos de Barbacoa', 90.00, '2025-07-28');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `mesas`
--

CREATE TABLE `mesas` (
  `id` int(11) NOT NULL,
  `nombre` varchar(50) NOT NULL,
  `estado` enum('libre','ocupada','reservada') DEFAULT 'libre',
  `capacidad` int(11) DEFAULT 4,
  `mesa_principal_id` int(11) DEFAULT NULL,
  `area` varchar(50) DEFAULT NULL,
  `tiempo_ocupacion_inicio` datetime DEFAULT NULL,
  `estado_reserva` enum('ninguna','reservada') DEFAULT 'ninguna',
  `nombre_reserva` varchar(100) DEFAULT NULL,
  `fecha_reserva` datetime DEFAULT NULL,
  `usuario_id` int(11) DEFAULT NULL,
  `area_id` int(11) DEFAULT NULL,
  `ticket_enviado` tinyint(1) DEFAULT 0,
  `alineacion_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `mesas`
--

INSERT INTO `mesas` (`id`, `nombre`, `estado`, `capacidad`, `mesa_principal_id`, `area`, `tiempo_ocupacion_inicio`, `estado_reserva`, `nombre_reserva`, `fecha_reserva`, `usuario_id`, `area_id`, `ticket_enviado`, `alineacion_id`) VALUES
(1, 'Mesa 1', 'libre', 4, NULL, 'Ala izquierda', '2025-09-09 12:51:56', 'ninguna', NULL, NULL, 6, 1, 0, NULL),
(2, 'Mesa 2', 'libre', 4, NULL, 'Ala derecha', NULL, 'ninguna', NULL, NULL, 2, 2, 0, 3),
(3, 'Mesa 3', 'libre', 6, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 5, 1, 0, 4),
(4, 'Mesa 4', 'libre', 4, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 4, 1, 0, NULL),
(5, 'Mesa 5', 'libre', 4, NULL, 'Ala derecha', NULL, 'ninguna', NULL, NULL, 17, 2, 0, NULL),
(6, 'Mesa 6', 'libre', 6, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 5, 1, 0, 3),
(7, 'Mesa 7', 'libre', 2, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 5, 1, 0, NULL),
(8, 'Mesa 8', 'libre', 4, NULL, 'Ala derecha', NULL, 'ninguna', NULL, NULL, 6, 2, 0, NULL),
(9, 'Mesa 9', 'libre', 4, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, NULL, 1, 0, 3),
(10, 'Mesa 10', 'libre', 6, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 1, 1, 0, NULL),
(11, 'Mesa 11', 'libre', 4, NULL, 'Ala derecha', NULL, 'ninguna', NULL, NULL, 5, 2, 0, NULL),
(12, 'Mesa 12', 'libre', 4, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 5, 1, 0, 3),
(13, 'Mesa 13', 'libre', 6, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 1, 1, 0, NULL),
(14, 'Mesa 14', 'libre', 4, NULL, 'Ala derecha', NULL, 'ninguna', NULL, NULL, 4, 2, 0, NULL),
(15, 'Mesa 15', 'libre', 4, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 17, 1, 0, 3),
(16, 'Mesa 16', 'libre', 2, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 17, 1, 0, NULL),
(17, 'Mesa 17', 'libre', 4, NULL, 'Ala derecha', NULL, 'ninguna', NULL, NULL, 6, 2, 0, NULL),
(18, 'Mesa 18', 'libre', 4, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 5, 1, 0, 3),
(19, 'Mesa 19', 'libre', 6, NULL, 'Ala izquierda', NULL, 'ninguna', NULL, NULL, 2, 1, 0, NULL),
(20, 'Mesa 20', 'libre', 4, NULL, 'Ala derecha', NULL, 'ninguna', NULL, NULL, 4, 2, 0, NULL);

--
-- Disparadores `mesas`
--
DELIMITER $$
CREATE TRIGGER `trg_log_asignacion_mesa` AFTER UPDATE ON `mesas` FOR EACH ROW BEGIN
    IF NEW.usuario_id <> OLD.usuario_id THEN
        INSERT INTO log_asignaciones_mesas (
            mesa_id,
            mesero_anterior_id,
            mesero_nuevo_id,
            fecha_cambio,
            usuario_que_asigna_id
        )
        VALUES (
            NEW.id,
            OLD.usuario_id,
            NEW.usuario_id,
            NOW(),
            @usuario_asignador_id
        );
    END IF;
END
$$
DELIMITER ;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `movimientos_caja`
--

CREATE TABLE `movimientos_caja` (
  `id` int(11) NOT NULL,
  `corte_id` int(11) DEFAULT NULL,
  `usuario_id` int(11) NOT NULL,
  `tipo_movimiento` enum('deposito','retiro') NOT NULL,
  `monto` decimal(10,2) NOT NULL,
  `motivo` text NOT NULL,
  `fecha` datetime DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `movimientos_caja`
--

INSERT INTO `movimientos_caja` (`id`, `corte_id`, `usuario_id`, `tipo_movimiento`, `monto`, `motivo`, `fecha`) VALUES
(7, 82, 1, 'deposito', 500.00, 'recarga', '2025-09-14 04:12:12'),
(8, 82, 1, 'retiro', 200.00, 'prueba', '2025-09-14 04:24:59');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `movimientos_insumos`
--

CREATE TABLE `movimientos_insumos` (
  `id` int(11) NOT NULL,
  `tipo` enum('entrada','salida','ajuste','traspaso') DEFAULT 'entrada',
  `usuario_id` int(11) DEFAULT NULL,
  `usuario_destino_id` int(11) DEFAULT NULL,
  `insumo_id` int(11) DEFAULT NULL,
  `cantidad` decimal(10,2) DEFAULT NULL,
  `observacion` text DEFAULT NULL,
  `fecha` datetime DEFAULT current_timestamp(),
  `qr_token` varchar(64) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `ofertas_dia`
--

CREATE TABLE `ofertas_dia` (
  `id` int(11) NOT NULL,
  `descripcion` text DEFAULT NULL,
  `vigente` tinyint(1) DEFAULT 1,
  `fecha` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `ofertas_dia`
--

INSERT INTO `ofertas_dia` (`id`, `descripcion`, `vigente`, `fecha`) VALUES
(1, '2x1 en aguas frescas', 1, '2025-07-28'),
(2, '10% de descuento en platillo del día', 1, '2025-07-28');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `productos`
--

CREATE TABLE `productos` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `precio` decimal(10,2) NOT NULL,
  `descripcion` text DEFAULT NULL,
  `existencia` int(11) DEFAULT 0,
  `activo` tinyint(1) DEFAULT 1,
  `imagen` varchar(255) DEFAULT NULL,
  `categoria_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `productos`
--

INSERT INTO `productos` (`id`, `nombre`, `precio`, `descripcion`, `existencia`, `activo`, `imagen`, `categoria_id`) VALUES
(4, 'Refresco 600ml', 20.00, 'Refresco embotellado', 2998, 1, NULL, 1),
(5, 'Rollo California', 120.00, 'Salmón, arroz, alga nori', 102, 1, NULL, 3),
(6, 'Guamuchilito', 109.00, 'Surimi, camarón empanizado, salsa de anguila', 102, 1, NULL, 8),
(7, 'Guerra', 125.00, 'Camarón, ajonjolí, aguacate, salsa de anguila', 102, 1, NULL, 8),
(8, 'Triton Roll', 125.00, 'Philadelphia, pepino, aguacate, surimi, atún ahumado, anguila, siracha', 102, 1, 'prod_68add2ff11cc1.jpg', 8),
(9, 'Mechas', 139.00, 'Philadelphia, pepino, aguacate, camarón, ajonjolí, kanikama, camarón empanizado,limón, sirracha, anguila, shisimi', 102, 1, NULL, 8),
(10, 'Supremo', 135.00, 'Surimi, philadelphia, ajonjolí, tampico,  pollo capeado, salsa de anguila', 102, 1, NULL, 8),
(11, 'Roka Crunch Roll', 119.00, 'Philadelphia, pepino, aguacate, camarón, surimi empanizado, zanahoria rallada, salsa de anguila', 102, 1, NULL, 8),
(12, 'Mar y Tierra', 105.00, 'Rollo relleno de carne y camarón.', 102, 1, NULL, 9),
(13, 'Cielo, Mar y Tierra', 109.00, 'Pollo, carne, camarón', 102, 1, NULL, 9),
(14, '3 Quesos', 115.00, 'Rollo de camarón, carne, base, queso americano\n y gratinado con queso chihuahua.', 102, 1, 'prod_68adcf8c73757.jpg', 9),
(15, 'Chiquilin Roll', 115.00, 'Relleno de base (philadelphia, pepino y\n aguacate) Por fuera topping de camarón\n empanizado especial, bañado en salsa de anguila\n y ajonjolí.', 102, 1, NULL, 9),
(16, 'Maki roll', 105.00, 'Rollo de 1 ingrediente a elegir (carne, tampico,\n pollo y camarón)', 101, 1, NULL, 9),
(17, 'Beef cheese', 119.00, 'Rollo de carne gratinado con queso spicy y\n ajonjolí.', 102, 1, NULL, 9),
(18, 'Cordon Blue', 115.00, 'Rollo relleno de carne y tocino forrado con\n philadelphia y gratinado con queso.', 102, 1, NULL, 9),
(19, 'Culichi Roll', 125.00, 'Rollo de carne con topping especial de tampico\n Tokyo empanizado coronado con camarón.', 102, 1, NULL, 9),
(20, 'Bacon Cheese', 125.00, 'Rollo de pollo por fuera gratinado con tocino.', 102, 1, 'prod_68add11ce2483.jpg', 9),
(21, 'Crunch Chicken', 125.00, 'Pollo empanizado, tocino, chile serrano, salsa bbq, salsa de anguila', 102, 1, NULL, 9),
(22, 'Kito', 119.00, 'Carne, tocino, queso, tampico', 102, 1, 'prod_68add69fe703c.jpg', 9),
(23, 'Norteño', 115.00, 'Camarón, tampico, queso, tocino, chile serrano', 102, 1, NULL, 9),
(24, 'Goloso Roll', 135.00, 'Res, pollo, tocino, queso o tampico', 102, 1, 'prod_68add37a08889.jpg', 9),
(25, 'Demon roll', 135.00, 'Res, tocino, toping demon (camarón enchiloso)', 102, 1, 'prod_68add435d40b1.jpg', 9),
(26, 'Nano Max', 245.00, 'Dedos de queso, dedos de surimi, carne, pollo, tocino, tampico, empanizado', 186, 1, NULL, 12),
(27, 'Nano XL', 325.00, 'Dedos de queso, dedosde surimi, carne pollo, tocino, queso, tampico, 1.5 kg', 107, 1, 'prod_68add3af36463.jpg', 12),
(28, 'Nano T-plus', 375.00, 'Dedos de queso, dedosde surimi, carne pollo, tocino, queso, tampico, 2 kg', 16, 1, NULL, 12),
(29, 'Chile Volcán', 85.00, 'Chile, 1 ingrediente a elegir, arroz, queso chihuahua,philadelphia', 427, 1, NULL, 10),
(30, 'Kushiagues', 75.00, 'Par de brochetas (camarón, pollo o surimi)', 2951, 1, NULL, 10),
(31, 'Dedos de Queso', 69.00, 'Queso, empanizado (5 piezas)', 199, 1, NULL, 10),
(32, 'Tostada Culichi', 75.00, 'Tostada, camarón, pulpo, callo, pepino, cebolla morada, masago, chile serrano, chile en polvo, jugo de aguachile', 1411, 1, 'prod_68add491b6f90.jpg', 10),
(33, 'Tostada tropical', 75.00, 'Tostada, atún, mango, camarón, callo, cebolla morada, chile en polvo, jugo de aguachile', 482, 1, NULL, 10),
(34, 'Empanada Horneada', 115.00, 'Tortilla de harina, carne, pollo, camarón,  mezcla de quesos, tampico, anguila y sirracha', 241, 1, NULL, 10),
(35, 'Rollitos', 75.00, 'Orden de 2 piezas, rellenos de philadelphia,\n queso chihuahua e ingrediente a elegir (res, pollo\n o camarón).', 2848, 1, 'prod_68add227e7037.jpg', 10),
(36, 'Gyozas', 95.00, 'Orden con 6 piezas pequeñas (Pueden ser de\n philadelphia y camarón o de pollo y verduras)', 2848, 1, NULL, 10),
(37, 'Papas a la francesa', 65.00, 'Papas a la francesa y cátsup ó aderezo especial', 0, 1, NULL, 10),
(38, 'Papas gajo', 75.00, 'Papas gajo y cátsup ó aderezo especial', 0, 1, NULL, 10),
(39, 'Ceviche Tokyo', 165.00, 'Cama de pepino, kanikama, camarón, aguacate, pulpo, jugo de aguachile', 141, 1, 'prod_68add2c342bb0.jpg', 3),
(40, 'Teriyaki krispy', 135.00, 'pollo empanizado, chile morrón, chile de arból, zanahoria, cebolla morada, cacahuate con salsa (salado)', 3000, 1, NULL, 3),
(41, 'Teriyaki', 139.00, 'Ingrediente a elegir, salteado de cebolla, zanahoria, calabaza, brócoli y coliflor, salsa teriyaki (dulce)', 3000, 1, NULL, 3),
(42, 'Pollo Mongol', 135.00, 'Pollo capeado, cebolla, zanahoria, apio, chile serrano, chile morrón, chile de arból, salsas orientales, montado en arroz blanco', 2994, 1, 'prod_68add8fa7fb9e.jpg', 3),
(43, 'Chow Mein Especial', 155.00, 'Pasta frita, camarón, carne, pollo, vegetales, salsas orientales', 2951, 1, 'prod_68adcfaa08c5a.jpg', 4),
(44, 'Chukasoba', 149.00, 'Camarón, pulpo, vegetales, pasta chukasoba', 2951, 1, NULL, 4),
(45, 'Fideo Yurey', 165.00, 'Fideo chino transparente, julianas de zanahoria y apio, cebolla, chile caribe y morrón y la proteína de tu elección', 2998, 1, NULL, 4),
(46, 'Udon spicy', 179.00, 'Julianas de zanahoria y cebolla, chile caribe, apio, chile de árbol, nuez de la india, ajonjolí, camarones capeados', 2996, 1, 'prod_68add7d1cd5d9.jpg', 4),
(47, 'Orange Chiken Tokyo', 149.00, 'Pollo capeado (300gr), graby de naranja, pepino, zanahoria, rodajas de naranja, ajonjolíPollo capeando (300gr) rebosado con graby de\n naranja con zanahoria, pepino y rodajas de naranja\n y ajonjolí', 2822, 1, NULL, 3),
(48, 'Udon Muchi', 125.00, 'Pasta udon, vegetales, camarón y pollo', 0, 1, NULL, 4),
(49, 'Tokyo ramen', 125.00, 'Pasta, vegetales, naruto, huevo, carne, camarón, fondo de res y cerdo', 0, 1, NULL, 4),
(50, 'Ramen Gran Meat', 125.00, 'Pasta, vegetales, trozos de carne sazonada con salsas orientales', 0, 1, NULL, 4),
(51, 'Ramen yasai', 115.00, 'Pasta, vegetales, fondo de res y cerdo', 0, 1, NULL, 4),
(52, 'Baby Ramen', 119.00, 'Pasta, vegetales, pollo a la plancha, salsas orientales, fondo de res y cerdo', 0, 1, NULL, 4),
(53, 'Cajun Ramen', 155.00, 'Fideos, vegetales, camarón gigante para pelar, fondo de res y cerdo, ajonjolí', 0, 1, NULL, 4),
(54, 'Gohan', 125.00, 'Arroz blanco, res y pollo, base de philadelphia y tampico con rodajas de aguacate, camarones empanizados, ajonjolí', 72, 1, NULL, 5),
(55, 'Gohan Krispy', 115.00, 'Arroz blanco, base de philadelphia, tampico y cubitos de aguacate, pollo y cebolla capeados, salsa de anguila, ajonjolí', 72, 1, 'prod_68add4bf039d2.jpg', 5),
(56, 'Yakimeshi', 115.00, 'Arroz frito, vegetales, carne, pollo y tocino, philadelphia, tampico, aguacate, ajonjolí', 0, 1, 'prod_68add0ace9c67.jpg', 5),
(57, 'Rollo Aguachile Especial', 125.00, 'Arroz frito, pollo empanizado, philadelphia, aguacate y tampico', 2848, 1, 'prod_68add7b73652c.jpg', 5),
(58, 'Bomba', 115.00, 'Bola de arroz, res, pollo, philadelphia, queso chihuahua, tampico , empanizada y cubierta de salsa de anguila', 2848, 1, 'prod_68add5bb666f3.jpg', 5),
(59, 'Menú kids 1', 79.00, '1/2 Rollo de pollo (6 piezas) y papas a la francesa', 100, 1, NULL, 3),
(60, 'Kid mini Yakimeshi', 85.00, 'Yakimeshi mini y papas a la francesa', 0, 1, NULL, 3),
(61, 'Menú Kids 3', 79.00, 'Dedos de queso (3 piezas) y papas a la francesa', 100, 1, NULL, 3),
(62, 'Chocoflan', 49.00, 'Porción de chocoflan', 30000, 1, NULL, 2),
(63, 'Pay de Queso', 49.00, 'Porción de pay de queso', 30000, 1, 'prod_68ae01fd0820f.jpg', 2),
(64, 'Helado Tempura', 79.00, 'Helado tempura', 30000, 1, NULL, 2),
(65, 'Postre Especial', 79.00, NULL, 30000, 1, 'prod_68ae00d2cd4af.jpg', 2),
(66, 'Té de Jazmín (Litro)', 33.00, 'Té verde con aroma a jazmín, servido en litro.', 30, 1, NULL, 1),
(67, 'Té de Jazmín (Refil)', 35.00, 'Té verde aromatizado con flores de jazmín.', 30, 1, NULL, 1),
(68, 'Limonada Natural', 35.00, 'Bebida de limón exprimido con agua y azúcar.', 2000, 1, NULL, 1),
(69, 'Limonada Mineral', 38.00, 'Bebida de limón con agua mineral y azúcar.', 147, 1, NULL, 1),
(70, 'Naranjada Natural', 35.00, 'Bebida de jugo de naranja con agua y azúcar.', 2000, 1, NULL, 1),
(71, 'Naranjada Mineral', 38.00, 'Refresco de naranja con agua mineral.', 147, 1, NULL, 1),
(72, 'Agua de Tamarindo', 35.00, 'Bebida dulce y ácida de tamarindo.', 29999, 1, NULL, 1),
(73, 'Agua Mineral (355ml)', 35.00, 'Agua con gas en envase pequeño.', 29400, 1, 'prod_68ae05aa8d01f.jpg', 1),
(74, 'Calpico', 35.00, 'Bebida japonesa dulce y láctea de yogur.', 97, 1, 'prod_68ae01959fac5.jpg', 1),
(75, 'Calpitamarindo', 39.00, NULL, 98, 1, NULL, 1),
(76, 'Refresco (335ml)', 29.00, 'Refresco embotellado', 29985, 1, 'prod_68ae07bd9ef3c.jpg', 1),
(77, 'Aderezo de Chipotle', 10.00, 'Salsa cremosa picante de chipotle.', 2894, 1, 'prod_68ae00b788642.jpg', 6),
(78, 'Aderezo de Cilantro', 15.00, 'Salsa cremosa con cilantro fresco.', 2996, 1, NULL, 6),
(79, 'Salsa Sriracha', 10.00, 'Alsa picante de chile, ajo y vinagre.', 1995, 1, 'prod_68ae083f538d5.jpg', 6),
(80, 'Jugo de Aguachile', 15.00, 'Salsa líquida de limón, chile y especias usada para marinar mariscos.', 0, 1, NULL, 6),
(81, 'Ranch', 15.00, 'Aderezo cremoso de hierbas y especias.', 3000, 1, 'prod_68ae011cd828d.jpg', 6),
(82, 'Búfalo', 15.00, 'Salsa picante de chile y mantequilla.', 2998, 1, 'prod_68ae0164e5f57.jpg', 6),
(83, 'BBQ', 15.00, 'Salsa dulce y ahumada para carnes.', 2999, 1, NULL, 6),
(84, 'Soya Extra', 10.00, 'Salsa de soja concentrada o adicional', 2000, 1, NULL, 6),
(85, 'Salsa de Anguila', 10.00, 'Salsa dulce y salada hecha con anguila y soja.', 1999, 1, NULL, 6),
(86, 'Cebollitas o Chiles', 10.00, NULL, 299600, 1, NULL, 6),
(87, 'Topping Horneado Especial', 20.00, 'Aderezo de chipotle, anguila y sriracha', 1929, 1, NULL, 7),
(88, 'Topping Kanikama', 35.00, '(Ensalada de cangrejo)', 998, 1, NULL, 7),
(89, 'Topping Tampico', 15.00, '(Ensalada de surimi)', 998, 1, NULL, 7),
(90, 'Topping Demon', 35.00, 'Camarón, tocino, quesos, serrano y chichimi', 491, 1, NULL, 7),
(91, 'Topping Chiquilín', 30.00, 'Camarón empanizado, anguila y ajonjolí', 470, 1, NULL, 7),
(92, 'Gladiador Roll', 139.00, 'Por dentro philadelphia, pepino y aguacate. Por fuera trozos de pulpo, queso spicy, shishimi y cebolla, bañado en salsa de anguila y ajonjolí. Rollo natural.', 102, 1, NULL, 13),
(93, 'Güerito Roll', 145.00, 'Por dentro camarón. Forrado con philadelphia y manchego, bañado en aderezo de chipotle, coronado con tocino, caribe y bañado en salsa sriracha. Empanizado.', 102, 1, 'prod_68add1fe92388.jpg', 13),
(94, 'Ebby Especial Roll', 145.00, 'Por dentro base, forrado con tampico cheese, bañado en aderezo de chipotle y coronado con camarón mariposa, aguacate, anguila y ajonjolí. Empanizado.', 102, 1, NULL, 13),
(95, 'Pakun Roll', 135.00, 'Relleno de tocino, por fuera topping de pollo y queso spicy, zanahoria. Acompañado de salsa anguila. Rollo natural.', 102, 1, NULL, 13),
(96, 'Rorris Roll', 135.00, 'Camarón y caribe por dentro, topping de tampico cheese, aguacate y bañados en salsa de anguila y ajonjolí. Empanizado.', 102, 1, NULL, 13),
(97, 'Royal Roll', 139.00, 'Carne y tocino por dentro, con topping de pollo. Empanizado, bañado con aderezo de chipotle, salsa de anguila y ajonjolí.', 102, 1, 'prod_68adcf0749621.jpg', 13),
(98, 'Larry Roll', 155.00, 'Rollo relleno de camarón, forrado con salmón. Topping de surimi finamente picado, spicy, coronado con atún fresco y bañado en salsa de anguila y ajonjolí.', 102, 1, 'prod_68add883e8ce7.jpg', 11),
(99, 'Aguachile Especial Roll', 155.00, 'Rollo relleno de philadelphia, pepino y aguacate. Forrado de chile serrano finamente picado, coronado con un aguachile especial de camarón, pulpo, callo y aguacate.', 102, 1, NULL, 11),
(100, 'Mordick Roll', 145.00, 'Rollo relleno de tocino, montado doble con queso gratinado, mezcla de quesos spicy, coronado con camarones empanizados y bañado en salsa de anguila y ajonjolí.', 102, 1, 'prod_68add0ea0033c.jpg', 11),
(101, 'Maney Roll', 165.00, 'Relleno de philadelphia, pepino y aguacate. Forrado de aguacate fresco y topping con camarón, medallón de atún, callo, mango y cebolla morada. Acompañado de salsa aguachile. Rollo natural.', 102, 1, 'prod_68add31666451.jpg', 11),
(102, 'Onigiri', 59.00, '1 Pieza de triángulo de arroz blanco, con un toque ligero de philadelphia, forrado de alga, cubierto de ajonjolí y relleno opcional de pollo con verduras (col morada y zanahoria) o atún con aderezo especial de mayonesa y cebollín.', 162, 1, 'prod_68add35c53114.jpg', 3),
(103, 'Dumplings', 95.00, 'Orden de 6 piezas de dumplings, rellenos de carne molida de cerdo. Sazonados orientalmente y acompañado con salsa macha.', 3000, 1, 'prod_68add5b64497c.jpg', 3),
(104, 'Boneless', 135.00, '250gr. De boneless con salsa a elegir (búfalo, bbq, mango habanero, mostaza miel o mermelada de chipotle).', 118, 1, 'prod_68ae02330ae4d.jpg', 3),
(105, 'Alitas', 135.00, '250gr. De alitas con salsa a elegir (búfalo, bbq, mango habanero, mostaza miel ó mermelada de chipotle).', 115, 1, 'prod_68add564480c8.jpg', 3),
(106, 'Sopa Pho', 149.00, 'Rico fondo de pollo con vegetales, pechuga de pollo, fideos chinos y chile de árbol. Coronado con 4 piezas de dumplings.', 500, 1, 'prod_68adce4f32265.jpg', 3),
(107, 'Yummy Roll', 159.00, 'Alga por fuera, relleno de camarón, philadelphia, pepino y aguacate. Gratinado con queso spicy de la casa. Coronado con camarón, aguacate y bañado en salsa de anguila y ajonjolí.', 102, 1, 'prod_68add0c25fd53.jpg', 3),
(108, 'Cebolla Caramelizada', 10.00, 'Cebolla Caramelizada', 150000, 1, NULL, 6),
(109, 'Kintaro', 102.00, 'Plato de sushi con atún graso picado toro y cebollín', 102, 1, NULL, NULL),
(110, 'Guamuchilito Especial', 123.00, 'Bebida preparada con jugo de guamúchil, combinada con alcohol, salsas y especias.', 102, 1, 'prod_68add0071ee2a.jpg', 11),
(111, 'Juny', 333.00, 'Juny', 102, 1, NULL, NULL),
(112, 'Pork Spicy', 122.00, 'Platillo de cerdo picante.', 108, 1, 'prod_68adcf27bc6a4.jpg', 8),
(120, 'Corona 1/2', 35.00, 'Cerveza helada', 29998, 1, 'prod_68adff38848ad.jpg', 1),
(121, 'Corona Golden Light 1/2', 35.00, 'Cerveza Golden helada', 30000, 1, 'prod_68adff1b2eb67.jpg', 1),
(122, 'Negra Modelo', 40.00, 'Cerveza negra helada', 30000, 1, 'prod_68adffa389931.jpg', 1),
(123, 'Modelo Especial', 40.00, 'Cerveza helada', 29993, 1, 'prod_68adfeeac5c57.jpg', 1),
(124, 'Bud Light', 35.00, 'Cerveza helada', 29996, 1, 'prod_68ae0111e5f9d.jpg', 1),
(125, 'Stella Artois', 45.00, 'Cerveza Helada', 30000, 1, 'prod_68ae081b893ed.jpg', 1),
(126, 'Ultra 1/2', 45.00, 'Cerveza helada', 30000, 1, 'prod_68ae015b16f22.jpg', 1),
(127, 'Michelob 1/2', 45.00, 'Cerveza helada', 30000, 1, NULL, 1),
(128, 'Vaso Chelado', 10.00, 'Vaso chelado', 1999, 1, NULL, 6),
(129, 'Vaso Michelado', 15.00, 'Vaso michelado', 1999, 1, NULL, 6),
(130, 'Vaso Clamato', 25.00, 'Vaso michelado', 1999, 1, NULL, 6),
(131, 'Cheese fresse', 109.00, 'Concha de papa gajo sazonada, bañada en delicioso queso y tozos de tocino; al horno', 119, 1, 'prod_68add54669456.jpg', 3),
(132, 'Charola Kyoyu Suru', 189.00, 'Camaron capeado, aros de cebolla y gyosas de carne de cerdo, acompañado de delicioso dip especial de la casa y salsa oriental', 0, 1, 'prod_68add217dc164.jpg', 3),
(133, 'Alitas Nudz', 149.00, 'Deliciosos 300grs De alitas, bañadas en salsa dulce con chile, sabor cacahuate y ajonjolí', -1, 1, NULL, 3),
(134, 'Edamames', 79.00, 'Vaina de frijol de soja preparado con picante, soya,sal y limon en una cama de zanahoria', 0, 1, 'prod_68add3a313461.jpg', 3),
(135, 'Crispy Chesse', 99.00, 'Rollo de 6 a 7 pz relleno de carne, philadelphia, pepino y aguacate, empanizado, gratinado spicy y trozos de tocino frito.', 129, 1, NULL, 9),
(136, 'Chummy Roll', 99.00, 'Rollo de 6 a 7 pz relleno de philadelphia, pepino y aguacate, coronado con tampico y camarón Empanizado, bando en salsa de Anguila y ajonjoli.', 129, 1, NULL, 9),
(137, 'Pollo kai', 0.00, 'Pollo capeado con ejote, fécula, chili bean y sazón oriental', 0, 1, NULL, 3),
(138, 'Yakimeshi roka', 0.00, 'Arroz con tampico, philadelphia, verduras, boneless y aguacate', 0, 1, NULL, 5),
(139, 'Kushiages (Pollo)', 75.00, 'Par de brochetas de pollo', 495, 1, NULL, 10),
(140, 'Kushiages (Surimi)', 75.00, 'Par de brochetas de surimi', 0, 1, NULL, 10),
(141, 'Kushiages (Camarón)', 75.00, 'Par de brochetas de camarón con philadelphia', 474, 1, NULL, 10),
(142, 'Rollitos (Pollo)', 75.00, 'Orden de 2 pz con pollo', 712, 1, NULL, 10),
(143, 'Rollitos (Res)', 75.00, 'Orden de 2 pz con res', 712, 1, NULL, 10),
(144, 'Rollitos (Camarón)', 75.00, 'Orden de 2 pz con camarón', 712, 1, NULL, 10),
(9000, 'Cargo por plataforma:', 30.00, 'Cargo por uso de plataforma web para pedidos', 99999, 1, NULL, 6),
(9001, 'ENVÍO – Repartidor casa', 30.00, 'Cargo por envío a domicilio (repartidor casa)', 99999, 1, NULL, 6),
(9004, 'Gyozas Pollo', 95.00, 'Orden de 6 pzas rellenas de pollo, zanahoria, jengibre y ajo', 0, 1, NULL, 10),
(9005, 'Gyozas Camarón', 95.00, 'Orden de 6 pzas rellenas de camarón y philadelphia', 983, 1, NULL, 10);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `proveedores`
--

CREATE TABLE `proveedores` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) DEFAULT NULL,
  `telefono` varchar(20) DEFAULT NULL,
  `direccion` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `proveedores`
--

INSERT INTO `proveedores` (`id`, `nombre`, `telefono`, `direccion`) VALUES
(1, 'La patita', NULL, ''),
(2, 'Sams', NULL, NULL),
(3, 'inix', NULL, NULL),
(4, 'mercado libre', NULL, NULL),
(5, 'Centauro', NULL, NULL),
(6, 'Fruteria los hermanos', NULL, NULL),
(7, 'Carmelita', NULL, NULL),
(8, 'Fruteria trebol', NULL, NULL),
(9, 'Gabriel', NULL, NULL),
(10, 'Limon nuevo', NULL, NULL),
(11, 'CPSmart', NULL, NULL),
(12, 'Quimicos San Ismael', NULL, NULL),
(13, 'Coca Cola', NULL, NULL),
(14, 'Cerveceria Modelo', NULL, NULL);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `qrs_insumo`
--

CREATE TABLE `qrs_insumo` (
  `id` int(11) NOT NULL,
  `token` varchar(64) NOT NULL,
  `json_data` text DEFAULT NULL,
  `estado` enum('pendiente','confirmado','anulado') DEFAULT 'pendiente',
  `creado_por` int(11) DEFAULT NULL,
  `creado_en` datetime DEFAULT current_timestamp(),
  `expiracion` datetime DEFAULT NULL,
  `pdf_envio` varchar(255) DEFAULT NULL,
  `pdf_recepcion` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `recetas`
--

CREATE TABLE `recetas` (
  `id` int(11) NOT NULL,
  `producto_id` int(11) DEFAULT NULL,
  `insumo_id` int(11) DEFAULT NULL,
  `cantidad` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `recetas`
--

INSERT INTO `recetas` (`id`, `producto_id`, `insumo_id`, `cantidad`) VALUES
(1, 5, 1, 190.00),
(2, 5, 2, 0.50),
(3, 5, 3, 100.00),
(4, 4, 4, 1.00),
(5, 4, 72, 10.00),
(6, 5, 75, 10.00),
(7, 5, 2, 0.50),
(30, 10, 7, 50.00),
(31, 10, 12, 35.00),
(32, 10, 16, 10.00),
(33, 10, 81, 80.00),
(34, 10, 76, 10.00),
(75, 25, 8, 40.00),
(84, 28, 25, 10.00),
(85, 28, 8, 10.00),
(86, 28, 81, 10.00),
(89, 30, 10, 10.00),
(90, 30, 9, 10.00),
(91, 30, 7, 10.00),
(116, 35, 12, 10.00),
(117, 35, 11, 10.00),
(118, 36, 12, 10.00),
(119, 36, 10, 10.00),
(120, 36, 9, 10.00),
(128, 40, 22, 10.00),
(129, 40, 34, 10.00),
(130, 40, 29, 10.00),
(131, 40, 52, 10.00),
(132, 41, 34, 10.00),
(133, 41, 53, 10.00),
(134, 41, 44, 10.00),
(135, 41, 57, 10.00),
(136, 41, 77, 10.00),
(137, 42, 55, 10.00),
(138, 42, 34, 10.00),
(139, 42, 35, 10.00),
(140, 42, 21, 10.00),
(141, 42, 22, 10.00),
(142, 42, 78, 10.00),
(143, 43, 70, 10.00),
(144, 43, 10, 10.00),
(145, 43, 9, 10.00),
(146, 43, 78, 10.00),
(147, 44, 10, 10.00),
(148, 44, 33, 10.00),
(149, 44, 69, 10.00),
(150, 45, 43, 10.00),
(151, 45, 64, 10.00),
(152, 45, 35, 10.00),
(153, 45, 55, 10.00),
(154, 45, 32, 10.00),
(155, 45, 67, 10.00),
(156, 46, 64, 10.00),
(157, 46, 55, 10.00),
(158, 46, 32, 10.00),
(159, 46, 35, 10.00),
(160, 46, 45, 10.00),
(161, 46, 38, 10.00),
(162, 46, 16, 10.00),
(163, 47, 62, 10.00),
(164, 47, 36, 10.00),
(165, 47, 34, 10.00),
(166, 47, 74, 10.00),
(167, 47, 16, 10.00),
(171, 49, 50, 10.00),
(172, 49, 47, 10.00),
(173, 49, 10, 10.00),
(174, 49, 61, 10.00),
(175, 49, 48, 10.00),
(177, 51, 61, 10.00),
(178, 51, 48, 10.00),
(202, 57, 12, 10.00),
(203, 57, 24, 10.00),
(204, 57, 81, 10.00),
(206, 58, 9, 10.00),
(207, 58, 12, 10.00),
(208, 58, 11, 10.00),
(209, 58, 81, 10.00),
(362, 25, 14, 30.00),
(367, 5, 90, 45.00),
(384, 14, 24, 25.00),
(385, 14, 10, 25.00),
(386, 14, 14, 40.00),
(387, 14, 36, 25.00),
(388, 14, 12, 45.00),
(389, 14, 15, 2.00),
(390, 14, 66, 80.00),
(391, 14, 1, 190.00),
(392, 14, 2, 0.50),
(393, 20, 24, 25.00),
(394, 20, 36, 25.00),
(395, 20, 12, 25.00),
(396, 20, 66, 80.00),
(397, 20, 21, 5.00),
(398, 20, 8, 70.00),
(399, 20, 1, 190.00),
(400, 20, 2, 0.50),
(401, 17, 87, 25.00),
(402, 17, 24, 25.00),
(403, 17, 14, 50.00),
(404, 17, 36, 25.00),
(405, 17, 12, 25.00),
(406, 17, 66, 80.00),
(407, 17, 80, 15.00),
(408, 17, 1, 190.00),
(409, 17, 2, 0.50),
(410, 5, 24, 25.00),
(411, 5, 10, 45.00),
(412, 5, 36, 25.00),
(413, 5, 12, 25.00),
(414, 18, 24, 25.00),
(415, 18, 14, 30.00),
(416, 18, 36, 25.00),
(417, 18, 12, 45.00),
(418, 18, 66, 80.00),
(419, 18, 8, 15.00),
(420, 18, 1, 190.00),
(421, 18, 2, 0.50),
(422, 21, 24, 25.00),
(423, 21, 19, 2.00),
(424, 21, 20, 4.00),
(425, 21, 92, 5.00),
(426, 21, 36, 25.00),
(427, 21, 12, 25.00),
(428, 21, 9, 50.00),
(429, 21, 91, 150.00),
(430, 21, 1, 190.00),
(431, 21, 2, 0.50),
(432, 19, 87, 25.00),
(433, 19, 24, 25.00),
(434, 19, 19, 20.00),
(435, 19, 10, 50.00),
(436, 19, 14, 50.00),
(437, 19, 36, 25.00),
(438, 19, 12, 25.00),
(439, 19, 81, 80.00),
(440, 19, 1, 190.00),
(441, 19, 2, 0.50),
(442, 13, 24, 25.00),
(443, 13, 10, 20.00),
(444, 13, 14, 20.00),
(445, 13, 36, 25.00),
(446, 13, 12, 25.00),
(447, 13, 9, 20.00),
(448, 13, 1, 190.00),
(449, 13, 2, 0.50),
(450, 25, 87, 25.00),
(451, 25, 24, 25.00),
(452, 25, 10, 25.00),
(453, 25, 36, 25.00),
(454, 25, 12, 25.00),
(455, 25, 66, 80.00),
(456, 25, 21, 10.00),
(457, 25, 80, 10.00),
(458, 25, 1, 190.00),
(459, 25, 2, 0.50),
(460, 94, 87, 25.00),
(461, 94, 24, 25.00),
(462, 94, 19, 10.00),
(463, 94, 10, 50.00),
(464, 94, 88, 80.00),
(465, 94, 36, 40.00),
(466, 94, 12, 25.00),
(467, 94, 1, 190.00),
(468, 94, 2, 0.50),
(469, 92, 87, 25.00),
(470, 92, 24, 25.00),
(471, 92, 88, 80.00),
(472, 92, 36, 40.00),
(473, 92, 12, 25.00),
(474, 92, 33, 40.00),
(475, 92, 1, 190.00),
(476, 92, 2, 0.50),
(477, 6, 24, 65.00),
(478, 6, 36, 25.00),
(479, 6, 12, 25.00),
(480, 6, 7, 50.00),
(481, 6, 81, 80.00),
(482, 6, 1, 190.00),
(483, 6, 2, 0.50),
(484, 110, 24, 65.00),
(485, 110, 10, 50.00),
(486, 110, 36, 25.00),
(487, 110, 12, 25.00),
(488, 110, 7, 50.00),
(489, 110, 81, 80.00),
(490, 110, 1, 190.00),
(491, 110, 2, 0.50),
(492, 93, 87, 25.00),
(493, 93, 24, 25.00),
(494, 93, 10, 45.00),
(495, 93, 92, 5.00),
(496, 93, 32, 5.00),
(497, 93, 36, 25.00),
(498, 93, 12, 25.00),
(499, 93, 66, 80.00),
(500, 93, 80, 10.00),
(501, 93, 8, 15.00),
(502, 93, 1, 190.00),
(503, 93, 2, 0.50),
(504, 7, 87, 25.00),
(505, 7, 24, 50.00),
(506, 7, 10, 45.00),
(507, 7, 90, 25.00),
(508, 7, 36, 25.00),
(509, 7, 12, 25.00),
(510, 7, 80, 15.00),
(511, 7, 1, 190.00),
(512, 7, 2, 0.50),
(513, 24, 24, 25.00),
(514, 24, 14, 20.00),
(515, 24, 88, 80.00),
(516, 24, 36, 25.00),
(517, 24, 12, 25.00),
(518, 24, 9, 20.00),
(519, 24, 81, 80.00),
(520, 24, 8, 15.00),
(521, 24, 1, 190.00),
(522, 24, 2, 0.50),
(523, 111, 93, 4.00),
(524, 111, 24, 25.00),
(525, 111, 19, 10.00),
(526, 111, 92, 10.00),
(527, 111, 36, 25.00),
(528, 111, 12, 40.00),
(529, 111, 3, 80.00),
(530, 111, 1, 190.00),
(531, 111, 2, 0.50),
(532, 109, 24, 25.00),
(533, 109, 92, 5.00),
(534, 109, 62, 6.00),
(535, 109, 36, 25.00),
(536, 109, 12, 25.00),
(537, 109, 91, 150.00),
(538, 109, 1, 190.00),
(539, 109, 2, 0.50),
(540, 22, 24, 25.00),
(541, 22, 14, 30.00),
(542, 22, 36, 25.00),
(543, 22, 12, 25.00),
(544, 22, 66, 80.00),
(545, 22, 81, 40.00),
(546, 22, 8, 15.00),
(547, 22, 1, 190.00),
(548, 22, 2, 0.50),
(549, 98, 87, 20.00),
(550, 98, 24, 25.00),
(551, 98, 40, 25.00),
(552, 98, 10, 45.00),
(553, 98, 92, 5.00),
(554, 98, 36, 25.00),
(555, 98, 12, 35.00),
(556, 98, 3, 30.00),
(557, 98, 80, 5.00),
(558, 98, 1, 190.00),
(559, 98, 2, 0.50),
(560, 101, 24, 60.00),
(561, 101, 40, 30.00),
(562, 101, 41, 30.00),
(563, 101, 10, 30.00),
(564, 101, 26, 30.00),
(565, 101, 36, 40.00),
(566, 101, 12, 35.00),
(567, 101, 33, 30.00),
(568, 101, 63, 4.00),
(569, 101, 1, 190.00),
(570, 101, 2, 0.50),
(571, 9, 87, 25.00),
(572, 9, 24, 25.00),
(573, 9, 10, 45.00),
(574, 9, 90, 50.00),
(575, 9, 92, 5.00),
(576, 9, 73, 50.00),
(577, 9, 65, 5.00),
(578, 9, 36, 25.00),
(579, 9, 12, 25.00),
(580, 9, 1, 190.00),
(581, 9, 2, 0.50),
(582, 100, 24, 25.00),
(583, 100, 19, 10.00),
(584, 100, 90, 50.00),
(585, 100, 88, 80.00),
(586, 100, 36, 25.00),
(587, 100, 12, 25.00),
(588, 100, 8, 35.00),
(589, 100, 1, 190.00),
(590, 100, 2, 0.50),
(591, 16, 24, 25.00),
(592, 16, 10, 45.00),
(593, 16, 14, 50.00),
(594, 16, 36, 25.00),
(595, 16, 12, 25.00),
(596, 16, 9, 50.00),
(597, 16, 81, 70.00),
(598, 16, 1, 190.00),
(599, 16, 2, 0.50),
(600, 12, 24, 25.00),
(601, 12, 10, 25.00),
(602, 12, 14, 40.00),
(603, 12, 36, 25.00),
(604, 12, 12, 25.00),
(605, 12, 1, 190.00),
(606, 12, 2, 0.50),
(607, 23, 24, 25.00),
(608, 23, 10, 25.00),
(609, 23, 36, 25.00),
(610, 23, 12, 25.00),
(611, 23, 66, 80.00),
(612, 23, 21, 10.00),
(613, 23, 81, 35.00),
(614, 23, 8, 25.00),
(615, 23, 1, 190.00),
(616, 23, 2, 0.50),
(617, 95, 87, 25.00),
(618, 95, 24, 25.00),
(619, 95, 88, 80.00),
(620, 95, 36, 25.00),
(621, 95, 12, 25.00),
(622, 95, 9, 40.00),
(623, 95, 8, 35.00),
(624, 95, 34, 20.00),
(625, 95, 1, 190.00),
(626, 95, 2, 0.50),
(637, 11, 87, 25.00),
(638, 11, 24, 25.00),
(639, 11, 90, 25.00),
(640, 11, 36, 40.00),
(641, 11, 12, 25.00),
(642, 11, 80, 10.00),
(643, 11, 7, 50.00),
(644, 11, 34, 25.00),
(645, 11, 1, 190.00),
(646, 11, 2, 0.50),
(647, 96, 87, 25.00),
(648, 96, 24, 50.00),
(649, 96, 10, 45.00),
(650, 96, 32, 5.00),
(651, 96, 88, 80.00),
(652, 96, 36, 25.00),
(653, 96, 12, 25.00),
(654, 96, 81, 80.00),
(655, 96, 1, 190.00),
(656, 96, 2, 0.50),
(657, 97, 87, 25.00),
(658, 97, 24, 25.00),
(659, 97, 19, 10.00),
(660, 97, 14, 30.00),
(661, 97, 88, 80.00),
(662, 97, 36, 25.00),
(663, 97, 12, 25.00),
(664, 97, 9, 25.00),
(665, 97, 8, 15.00),
(666, 97, 1, 190.00),
(667, 97, 2, 0.50),
(668, 10, 87, 25.00),
(669, 10, 24, 25.00),
(670, 10, 36, 25.00),
(671, 10, 91, 50.00),
(672, 10, 80, 10.00),
(673, 10, 1, 190.00),
(674, 10, 2, 0.50),
(675, 8, 87, 20.00),
(676, 8, 24, 25.00),
(677, 8, 51, 70.00),
(678, 8, 92, 5.00),
(679, 8, 36, 40.00),
(680, 8, 12, 25.00),
(681, 8, 80, 5.00),
(682, 8, 7, 5.00),
(683, 8, 1, 190.00),
(684, 8, 2, 0.50),
(685, 107, 87, 25.00),
(686, 107, 24, 75.00),
(687, 107, 10, 50.00),
(688, 107, 36, 25.00),
(689, 107, 12, 25.00),
(690, 107, 9, 25.00),
(691, 107, 66, 80.00),
(692, 107, 21, 5.00),
(693, 107, 80, 10.00),
(694, 107, 8, 15.00),
(695, 107, 1, 190.00),
(696, 107, 2, 0.50),
(697, 15, 87, 40.00),
(698, 15, 24, 25.00),
(699, 15, 90, 80.00),
(700, 15, 36, 40.00),
(701, 15, 12, 25.00),
(702, 15, 1, 190.00),
(703, 15, 2, 0.50),
(712, 62, 95, 1.00),
(713, 63, 96, 1.00),
(714, 64, 97, 1.00),
(715, 65, 98, 1.00),
(716, 77, 87, 10.00),
(717, 85, 19, 15.00),
(718, 128, 56, 15.00),
(719, 130, 56, 15.00),
(720, 129, 56, 15.00),
(721, 120, 101, 1.00),
(722, 121, 102, 1.00),
(723, 127, 108, 1.00),
(724, 126, 107, 1.00),
(725, 122, 103, 1.00),
(726, 123, 104, 1.00),
(727, 124, 105, 1.00),
(728, 125, 106, 1.00),
(729, 108, 55, 0.20),
(730, 86, 45, 0.10),
(731, 86, 55, 0.10),
(732, 76, 72, 1.00),
(733, 106, 45, 0.20),
(734, 106, 53, 20.00),
(735, 106, 34, 20.00),
(736, 106, 49, 4.00),
(737, 106, 73, 60.00),
(738, 106, 35, 10.00),
(748, 105, 109, 250.00),
(753, 83, 20, 10.00),
(754, 81, 110, 10.00),
(755, 82, 99, 10.00),
(756, 84, 30, 15.00),
(757, 91, 90, 60.00),
(758, 91, 16, 2.00),
(759, 91, 19, 10.00),
(760, 90, 10, 60.00),
(761, 90, 8, 30.00),
(762, 90, 11, 30.00),
(763, 90, 21, 10.00),
(764, 90, 112, 5.00),
(765, 88, 23, 30.00),
(766, 88, 89, 30.00),
(767, 89, 7, 30.00),
(768, 89, 89, 30.00),
(773, 87, 19, 10.00),
(774, 87, 80, 15.00),
(775, 87, 87, 15.00),
(780, 104, 115, 250.00),
(781, 104, 89, 30.00),
(782, 102, 1, 120.00),
(783, 102, 12, 30.00),
(784, 102, 2, 0.50),
(785, 102, 16, 5.00),
(786, 102, 36, 25.00),
(787, 102, 34, 30.00),
(788, 102, 92, 20.00),
(789, 99, 1, 190.00),
(790, 99, 36, 40.00),
(791, 99, 12, 30.00),
(792, 99, 2, 0.50),
(793, 99, 45, 10.00),
(794, 99, 94, 40.00),
(795, 99, 33, 40.00),
(796, 99, 24, 30.00),
(797, 79, 80, 15.00),
(798, 73, 117, 1.00),
(803, 78, 118, 10.00),
(804, 78, 89, 10.00),
(810, 66, 119, 1000.00),
(811, 67, 119, 1000.00),
(812, 135, 1, 150.00),
(813, 135, 2, 0.50),
(814, 135, 36, 25.00),
(815, 135, 12, 25.00),
(816, 135, 24, 25.00),
(817, 135, 17, 25.00),
(818, 135, 8, 25.00),
(819, 136, 1, 150.00),
(820, 136, 2, 0.50),
(821, 136, 36, 25.00),
(822, 136, 12, 25.00),
(823, 136, 24, 25.00),
(824, 136, 90, 25.00),
(825, 136, 81, 10.00),
(826, 136, 16, 2.00),
(827, 103, 59, 10.00),
(828, 134, 114, 150.00),
(829, 134, 112, 4.00),
(830, 134, 30, 60.00),
(831, 134, 65, 60.00),
(832, 134, 174, 60.00),
(833, 134, 16, 3.00),
(834, 131, 28, 250.00),
(835, 131, 88, 80.00),
(836, 131, 66, 80.00),
(837, 131, 8, 25.00),
(838, 131, 87, 15.00),
(839, 131, 92, 10.00),
(840, 60, 1, 270.00),
(841, 60, 34, 25.00),
(842, 60, 53, 25.00),
(843, 60, 181, 1.00),
(844, 60, 180, 30.00),
(845, 60, 175, 3.00),
(846, 60, 176, 3.00),
(847, 133, 109, 300.00),
(848, 133, 16, 5.00),
(849, 133, 21, 20.00),
(850, 133, 182, 40.00),
(851, 133, 56, 5.00),
(852, 133, 19, 6.00),
(853, 137, 177, 120.00),
(854, 137, 9, 150.00),
(855, 137, 38, 80.00),
(856, 137, 178, 20.00),
(857, 137, 20, 3.00),
(858, 137, 179, 3.00),
(859, 137, 35, 3.00),
(860, 137, 180, 30.00),
(861, 137, 16, 3.00),
(862, 137, 92, 10.00),
(863, 138, 1, 270.00),
(864, 138, 81, 80.00),
(865, 138, 12, 15.00),
(866, 138, 92, 10.00),
(867, 138, 16, 3.00),
(868, 138, 53, 25.00),
(869, 138, 34, 25.00),
(870, 138, 176, 3.00),
(871, 138, 183, 1.00),
(872, 138, 24, 50.00),
(873, 54, 81, 80.00),
(874, 54, 12, 15.00),
(875, 54, 1, 270.00),
(876, 54, 9, 25.00),
(877, 54, 14, 25.00),
(878, 54, 8, 15.00),
(879, 54, 24, 50.00),
(880, 54, 10, 60.00),
(881, 54, 92, 1.00),
(882, 54, 16, 3.00),
(883, 55, 1, 270.00),
(884, 55, 81, 80.00),
(885, 55, 12, 15.00),
(886, 55, 24, 50.00),
(887, 55, 55, 60.00),
(888, 55, 91, 150.00),
(889, 55, 19, 2.00),
(890, 55, 16, 3.00),
(891, 112, 2, 0.50),
(892, 112, 1, 180.00),
(893, 112, 12, 25.00),
(894, 112, 14, 40.00),
(895, 112, 24, 10.00),
(896, 112, 66, 80.00),
(897, 112, 80, 10.00),
(898, 112, 87, 10.00),
(899, 112, 8, 15.00),
(900, 132, 14, 40.00),
(901, 132, 49, 6.00),
(902, 132, 55, 100.00),
(903, 132, 54, 4.00),
(904, 132, 93, 2.00),
(905, 132, 185, 200.00),
(906, 132, 184, 1.00),
(907, 56, 53, 50.00),
(908, 56, 34, 50.00),
(909, 56, 1, 400.00),
(910, 56, 9, 150.00),
(911, 56, 14, 75.00),
(912, 56, 8, 50.00),
(913, 56, 176, 5.00),
(914, 56, 180, 60.00),
(915, 56, 81, 80.00),
(916, 56, 24, 50.00),
(917, 56, 92, 10.00),
(918, 56, 16, 3.00),
(919, 33, 27, 1.00),
(920, 33, 10, 35.00),
(921, 33, 26, 35.00),
(922, 33, 29, 10.00),
(923, 33, 41, 20.00),
(924, 33, 40, 40.00),
(925, 33, 63, 4.00),
(926, 33, 56, 5.00),
(927, 33, 87, 60.00),
(928, 33, 37, 10.00),
(929, 37, 28, 240.00),
(930, 37, 175, 2.00),
(931, 39, 36, 200.00),
(932, 39, 33, 50.00),
(933, 39, 10, 150.00),
(934, 39, 41, 30.00),
(935, 39, 29, 60.00),
(936, 39, 23, 60.00),
(937, 39, 24, 80.00),
(938, 39, 63, 10.00),
(939, 39, 56, 5.00),
(940, 29, 32, 1.00),
(941, 29, 12, 15.00),
(942, 29, 1, 10.00),
(943, 29, 88, 70.00),
(944, 29, 66, 50.00),
(945, 29, 19, 1.00),
(946, 29, 87, 60.00),
(947, 29, 92, 10.00),
(948, 29, 56, 3.00),
(949, 9004, 49, 6.00),
(950, 9004, 9, 10.00),
(951, 9004, 34, 6.00),
(952, 9004, 187, 1.00),
(953, 9004, 186, 1.00),
(954, 9005, 49, 6.00),
(955, 9005, 12, 10.00),
(956, 9005, 10, 30.00),
(957, 38, 28, 250.00),
(958, 38, 175, 2.00),
(959, 31, 11, 150.00),
(960, 32, 27, 1.00),
(961, 32, 29, 10.00),
(962, 32, 41, 5.00),
(963, 32, 33, 7.00),
(964, 32, 36, 20.00),
(965, 32, 10, 15.00),
(966, 32, 63, 4.00),
(967, 32, 56, 5.00),
(968, 34, 14, 30.00),
(969, 34, 9, 30.00),
(970, 34, 10, 30.00),
(971, 34, 12, 5.00),
(972, 34, 81, 80.00),
(973, 34, 88, 80.00),
(974, 34, 87, 120.00),
(975, 34, 80, 30.00),
(976, 34, 19, 1.00),
(977, 34, 16, 5.00),
(978, 34, 92, 10.00),
(1100, 139, 9, 60.00),
(1101, 140, 7, 140.00),
(1102, 141, 10, 50.00),
(1103, 141, 12, 60.00),
(1104, 142, 82, 2.00),
(1105, 142, 12, 40.00),
(1106, 142, 66, 40.00),
(1107, 142, 9, 30.00),
(1108, 143, 82, 2.00),
(1109, 143, 12, 40.00),
(1110, 143, 66, 40.00),
(1111, 143, 14, 30.00),
(1112, 144, 82, 2.00),
(1113, 144, 12, 40.00),
(1114, 144, 66, 40.00),
(1115, 144, 10, 30.00),
(1116, 48, 34, 50.00),
(1117, 48, 53, 50.00),
(1118, 48, 189, 30.00),
(1119, 48, 46, 200.00),
(1120, 48, 9, 150.00),
(1121, 48, 10, 40.00),
(1122, 48, 61, 500.00),
(1123, 48, 188, 40.00),
(1124, 48, 16, 2.00),
(1125, 48, 92, 10.00),
(1131, 50, 69, 150.00),
(1132, 50, 34, 50.00),
(1133, 50, 53, 50.00),
(1134, 50, 177, 40.00),
(1135, 50, 61, 500.00),
(1136, 50, 14, 80.00),
(1137, 50, 188, 40.00),
(1138, 50, 16, 2.00),
(1139, 50, 92, 10.00),
(1146, 52, 69, 150.00),
(1147, 52, 34, 50.00),
(1148, 52, 53, 50.00),
(1149, 52, 189, 30.00),
(1150, 52, 9, 150.00),
(1151, 52, 61, 500.00),
(1152, 52, 188, 40.00),
(1153, 52, 16, 2.00),
(1154, 52, 92, 10.00),
(1161, 28, 1, 1150.00),
(1162, 28, 2, 2.00),
(1163, 28, 12, 120.00),
(1164, 28, 24, 40.00),
(1165, 28, 36, 80.00),
(1166, 28, 14, 130.00),
(1167, 28, 8, 80.00),
(1168, 28, 9, 130.00),
(1169, 28, 7, 420.00),
(1170, 28, 25, 6.00),
(1171, 28, 11, 150.00),
(1172, 28, 87, 50.00),
(1173, 28, 80, 30.00),
(1174, 28, 92, 20.00),
(1175, 28, 16, 5.00),
(1176, 49, 69, 150.00),
(1177, 49, 10, 40.00),
(1178, 49, 14, 50.00),
(1179, 49, 61, 500.00),
(1180, 49, 64, 50.00),
(1181, 49, 53, 50.00),
(1182, 49, 189, 30.00),
(1183, 49, 16, 2.00),
(1184, 49, 92, 10.00),
(1185, 51, 69, 150.00),
(1186, 51, 64, 50.00),
(1187, 51, 53, 50.00),
(1188, 51, 44, 50.00),
(1189, 51, 177, 50.00),
(1190, 51, 190, 50.00),
(1191, 51, 191, 30.00),
(1192, 51, 61, 500.00),
(1193, 51, 16, 2.00),
(1194, 51, 92, 10.00),
(1195, 26, 1, 0.75),
(1196, 26, 2, 1.00),
(1197, 26, 12, 90.00),
(1198, 26, 24, 40.00),
(1199, 26, 36, 40.00),
(1200, 26, 25, 2.00),
(1201, 26, 59, 2.00),
(1202, 26, 9, 100.00),
(1203, 26, 14, 100.00),
(1204, 26, 8, 80.00),
(1205, 26, 81, 160.00),
(1210, 27, 1, 0.95),
(1211, 27, 2, 1.50),
(1212, 27, 12, 100.00),
(1213, 27, 36, 60.00),
(1214, 27, 24, 40.00),
(1215, 27, 8, 80.00),
(1216, 27, 14, 110.00),
(1217, 27, 9, 110.00),
(1218, 27, 7, 280.00),
(1219, 27, 25, 4.00),
(1220, 27, 66, 0.10),
(1221, 27, 87, 50.00),
(1222, 27, 80, 20.00),
(1223, 27, 92, 15.00),
(1224, 27, 16, 2.00),
(1225, 53, 191, 0.03),
(1226, 53, 60, 150.00),
(1227, 53, 177, 30.00),
(1228, 53, 54, 4.00),
(1229, 53, 190, 30.00),
(1230, 53, 61, 500.00),
(1231, 53, 92, 10.00),
(1232, 53, 16, 2.00),
(1240, 69, 65, 60.00),
(1241, 69, NULL, 120.00),
(1242, 69, 117, 200.00),
(1243, 69, NULL, 125.00),
(1247, 68, 65, 60.00),
(1248, 68, NULL, 120.00),
(1249, 68, NULL, 200.00),
(1250, 68, NULL, 125.00),
(1254, 74, 113, 90.00),
(1255, 74, 117, 300.00),
(1256, 74, NULL, 125.00),
(1257, 72, NULL, 90.00),
(1258, 72, NULL, 500.00),
(1259, 72, NULL, 125.00),
(1260, 70, NULL, 120.00),
(1261, 70, NULL, 60.00),
(1262, 70, NULL, 125.00),
(1263, 70, NULL, 200.00),
(1267, 71, NULL, 120.00),
(1268, 71, NULL, 60.00),
(1269, 71, NULL, 125.00),
(1270, 71, 117, 200.00),
(1274, 75, NULL, 45.00),
(1275, 75, 113, 45.00),
(1276, 75, NULL, 125.00),
(1277, 75, 117, 300.00);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `repartidores`
--

CREATE TABLE `repartidores` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) DEFAULT NULL,
  `telefono` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `repartidores`
--

INSERT INTO `repartidores` (`id`, `nombre`, `telefono`) VALUES
(1, 'Didi', '555-000-1111'),
(2, 'Rappi', '555-999-2222'),
(3, 'Uber', NULL),
(4, 'Repartidor casa', NULL);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `rutas`
--

CREATE TABLE `rutas` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `path` varchar(255) NOT NULL,
  `tipo` enum('link','dropdown','dropdown-item') NOT NULL,
  `grupo` varchar(50) DEFAULT NULL,
  `orden` int(11) DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `rutas`
--

INSERT INTO `rutas` (`id`, `nombre`, `path`, `tipo`, `grupo`, `orden`) VALUES
(1, 'Inicio', '/vistas/index.php', 'link', NULL, 1),
(2, 'Productos', '#', 'dropdown', 'Productos', 3),
(3, 'Insumos', '/vistas/insumos/insumos.php', 'dropdown-item', 'Productos', 1),
(4, 'Inventario', '/vistas/inventario/inventario.php', 'dropdown-item', 'Productos', 2),
(5, 'Recetas', '/vistas/recetas/recetas.php', 'dropdown-item', 'Productos', 3),
(6, 'Cocina', '/vistas/cocina/cocina2.php', 'link', NULL, 4),
(7, 'Ventas', '/vistas/ventas/ventas.php', 'link', NULL, 5),
(8, 'Mover', '/vistas/mover/mover.php', 'dropdown-item', 'Más', 14),
(9, 'Repartos', '/vistas/repartidores/repartos.php', 'link', NULL, 7),
(10, 'Mesas', '/vistas/mesas/mesas.php', 'link', NULL, 8),
(11, 'Más', '#', 'dropdown', 'Más', 9),
(12, 'Horarios', '/vistas/horarios/horarios.php', 'dropdown-item', 'Más', 1),
(13, 'Ticket', '/vistas/ventas/ticket.php', 'dropdown-item', 'Más', 2),
(14, 'Reportes', '/vistas/reportes/reportes.php', 'dropdown-item', 'Más', 3),
(15, 'Ayuda', '/vistas/ayuda.php', 'link', NULL, 10),
(18, 'Generar QR', '/vistas/bodega/generar_qr.php', 'dropdown-item', 'Más', 4),
(19, 'Recibir QR', '/vistas/bodega/recepcion_qr.php', 'dropdown-item', 'Más', 5),
(20, 'Meseros', '/vistas/mesas/asignar.php', 'link', NULL, 11),
(21, 'Reporteria', '/vistas/reportes/vistas_db.php', 'dropdown-item', 'Más', 13),
(22, 'Usuarios', '/vistas/usuarios/usuarios.php', 'dropdown-item', 'Más', 6),
(23, 'Rutas', '/vistas/rutas/rutas.php', 'dropdown-item', 'Más', 7),
(24, 'Permisos', '/vistas/rutas/urutas.php', 'dropdown-item', 'Más', 8),
(25, 'CorteC', '/vistas/insumos/cortes.php', 'dropdown-item', 'Más', 9),
(26, 'Proveedores', '/vistas/insumos/proveedores.php', 'dropdown-item', 'Más', 10),
(27, 'Promos', '/vistas/promociones/promociones.php', 'dropdown-item', 'Más', 11),
(28, 'Facturas', '/vistas/facturas/masiva.php', 'dropdown-item', 'Más', 12),
(29, 'Dashboard', '/vistas/dashboard/dash.php', 'dropdown-item', 'Más', 14),
(30, 'Bancos', '/vistas/dashboard/bancos.php', 'dropdown-item', 'Más', 15),
(31, 'Sedes', '/vistas/dashboard/sedes.php', 'dropdown-item', 'Más', 16),
(32, 'Tarjetas', '/vistas/dashboard/tarjetas.php', 'dropdown-item', 'Más', 17);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `sedes`
--

CREATE TABLE `sedes` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `direccion` text NOT NULL,
  `rfc` varchar(20) NOT NULL,
  `telefono` varchar(20) NOT NULL,
  `correo` varchar(100) DEFAULT NULL,
  `web` varchar(100) DEFAULT NULL,
  `activo` tinyint(1) DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `sedes`
--

INSERT INTO `sedes` (`id`, `nombre`, `direccion`, `rfc`, `telefono`, `correo`, `web`, `activo`) VALUES
(1, 'Forestal', 'Blvd. Luis Donaldo Colosio #317, Fracc. La Forestal ', 'VEAJ9408188U9', '6183222352', 'ventas@tokyo.com', 'tokyosushiprime.com', 1),
(2, 'Domingo Arrieta', 'Chabacanos SN-5, El Naranjal, 34190 Durango, Dgo.', 'VEAJ9408188U9', '6181690319', 'ventas@tokyo.com', 'tokyosushiprime.com', 1);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tickets`
--

CREATE TABLE `tickets` (
  `id` int(11) NOT NULL,
  `venta_id` int(11) NOT NULL,
  `folio` int(11) NOT NULL,
  `serie_id` int(11) DEFAULT NULL,
  `total` decimal(10,2) NOT NULL,
  `descuento` decimal(10,2) NOT NULL DEFAULT 0.00,
  `fecha` datetime DEFAULT current_timestamp(),
  `usuario_id` int(11) DEFAULT NULL,
  `monto_recibido` decimal(10,2) DEFAULT 0.00,
  `tipo_pago` enum('efectivo','boucher','cheque') DEFAULT 'efectivo',
  `sede_id` int(11) DEFAULT NULL,
  `mesa_nombre` varchar(50) DEFAULT NULL,
  `mesero_nombre` varchar(100) DEFAULT NULL,
  `fecha_inicio` datetime DEFAULT NULL,
  `fecha_fin` datetime DEFAULT NULL,
  `tiempo_servicio` int(11) DEFAULT NULL COMMENT 'Minutos de servicio',
  `nombre_negocio` varchar(100) DEFAULT NULL,
  `direccion_negocio` text DEFAULT NULL,
  `rfc_negocio` varchar(20) DEFAULT NULL,
  `telefono_negocio` varchar(20) DEFAULT NULL,
  `tipo_entrega` enum('mesa','domicilio','rapido') DEFAULT 'mesa',
  `tarjeta_marca_id` int(11) DEFAULT NULL,
  `tarjeta_banco_id` int(11) DEFAULT NULL,
  `boucher` varchar(50) DEFAULT NULL,
  `cheque_numero` varchar(50) DEFAULT NULL,
  `cheque_banco_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `tickets`
--

INSERT INTO `tickets` (`id`, `venta_id`, `folio`, `serie_id`, `total`, `descuento`, `fecha`, `usuario_id`, `monto_recibido`, `tipo_pago`, `sede_id`, `mesa_nombre`, `mesero_nombre`, `fecha_inicio`, `fecha_fin`, `tiempo_servicio`, `nombre_negocio`, `direccion_negocio`, `rfc_negocio`, `telefono_negocio`, `tipo_entrega`, `tarjeta_marca_id`, `tarjeta_banco_id`, `boucher`, `cheque_numero`, `cheque_banco_id`) VALUES
(197, 344, 2066, 2, 347.17, 0.00, '2025-09-14 03:59:12', NULL, 347.17, 'boucher', 1, 'Venta rápida', 'Administrador', NULL, '2025-09-14 03:59:12', 0, 'Forestal', 'Blvd. Luis Donaldo Colosio #317, Fracc. La Forestal ', 'VEAJ9408188U9', '6183222352', 'rapido', 2, 6, 'ord_2ygDMFAXS6gAE5BDZ', NULL, NULL),
(198, 345, 2067, 2, 379.00, 0.00, '2025-09-14 04:02:34', NULL, 300.00, 'efectivo', 1, 'N/A', 'repartidor 1', NULL, '2025-09-14 04:02:34', 0, 'Forestal', 'Blvd. Luis Donaldo Colosio #317, Fracc. La Forestal ', 'VEAJ9408188U9', '6183222352', 'domicilio', NULL, NULL, NULL, NULL, NULL);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `ticket_descuentos`
--

CREATE TABLE `ticket_descuentos` (
  `id` int(11) NOT NULL,
  `ticket_id` int(11) NOT NULL,
  `tipo` enum('cortesia','porcentaje','monto_fijo','promocion') NOT NULL,
  `venta_detalle_id` int(11) DEFAULT NULL,
  `porcentaje` decimal(5,2) DEFAULT NULL,
  `monto` decimal(10,2) NOT NULL DEFAULT 0.00,
  `motivo` varchar(255) DEFAULT NULL,
  `usuario_id` int(11) DEFAULT NULL,
  `catalogo_promo_id` int(11) NOT NULL,
  `creado_en` datetime NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `ticket_detalles`
--

CREATE TABLE `ticket_detalles` (
  `id` int(11) NOT NULL,
  `ticket_id` int(11) NOT NULL,
  `producto_id` int(11) NOT NULL,
  `cantidad` int(11) NOT NULL,
  `precio_unitario` decimal(10,2) DEFAULT NULL,
  `subtotal` decimal(10,2) GENERATED ALWAYS AS (`cantidad` * `precio_unitario`) STORED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `ticket_detalles`
--

INSERT INTO `ticket_detalles` (`id`, `ticket_id`, `producto_id`, `cantidad`, `precio_unitario`) VALUES
(318, 197, 77, 1, 10.00),
(319, 197, 14, 1, 115.00),
(320, 197, 105, 1, 135.00),
(321, 197, 124, 1, 35.00),
(322, 197, 74, 1, 35.00),
(323, 197, 9000, 1, 17.17),
(324, 198, 15, 1, 115.00),
(325, 198, 13, 1, 109.00),
(326, 198, 16, 1, 105.00),
(327, 198, 9001, 1, 50.00);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `usuarios`
--

CREATE TABLE `usuarios` (
  `id` int(11) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `usuario` varchar(50) NOT NULL,
  `contrasena` varchar(255) NOT NULL,
  `rol` enum('cajero','mesero','admin','repartidor','cocinero','barra','alimentos') NOT NULL,
  `activo` tinyint(1) DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `usuarios`
--

INSERT INTO `usuarios` (`id`, `nombre`, `usuario`, `contrasena`, `rol`, `activo`) VALUES
(1, 'Administrador', 'admin', 'admin', 'admin', 1),
(2, 'Javier Emanuel lopez lozano', 'JavierE', 'd033e22ae348aeb5660fc2140aec35850c4da997', 'mesero', 1),
(3, 'maria jose valle tovar', 'MariaJ', 'admin', 'cajero', 1),
(4, 'juan hernesto ortega Almanza', 'juan', 'admin', 'mesero', 1),
(5, 'gilberto ozuna carrillo', 'Gil', 'admin', 'mesero', 1),
(6, 'alinne Guadalupe Gurrola ramirez', 'Alinne', 'd033e22ae348aeb5660fc2140aec35850c4da997', 'mesero', 1),
(7, 'Mesas general', 'mesas', 'admin', 'mesero', 1),
(8, 'Jose Angel Valdez Flores', 'AngelV', 'admin', 'cocinero', 1),
(9, 'Daniel Gutierrez Amador', 'DaniG', 'admin', 'cocinero', 1),
(10, 'Edson Darihec Reyes Villa', 'Darihec ', 'admin', 'cocinero', 1),
(11, 'Hector Osbaldo Hernandez Orona', 'HectorO', 'admin', 'cocinero', 1),
(12, 'Henry Adahyr Coronel Gamiz', 'Henry ', 'admin', 'cocinero', 1),
(13, 'Jose Arturo Montoya Campos', 'JoseA', 'admin', 'cocinero', 1),
(14, 'Kevin de Jesus Rosales Valles', 'KevinJ', 'admin', 'cocinero', 1),
(15, 'Roberto Garcia Soto', 'RobertoG', 'admin', 'cocinero', 1),
(16, 'Luis Varela Rueda', 'LuisV', 'admin', 'cocinero', 1),
(17, 'Jesus', 'Jesus', 'admin', 'mesero', 1),
(18, 'Andrea Jaqueline perez arrellano ', 'AndreaJ', 'admin', 'cajero', 1),
(31, 'Andrea ontivero escalera', 'AndreaO', 'admin', 'cajero', 1),
(32, 'Cajero General', 'CajeroG', 'admin', 'cajero', 1),
(33, 'Cocina General', 'CocinaG', 'admin', 'alimentos', 1),
(34, 'Barra General', 'BarraG', 'admin', 'barra', 1),
(35, 'repartidor 1', 'Repartidor1', 'admin', 'repartidor', 1),
(36, 'Repartidor2', 'Repartidor2', 'admin', 'repartidor', 1);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `usuario_ruta`
--

CREATE TABLE `usuario_ruta` (
  `id` int(11) NOT NULL,
  `usuario_id` int(11) NOT NULL,
  `ruta_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `usuario_ruta`
--

INSERT INTO `usuario_ruta` (`id`, `usuario_id`, `ruta_id`) VALUES
(100, 1, 1),
(105, 1, 2),
(101, 1, 3),
(103, 1, 4),
(106, 1, 5),
(108, 1, 6),
(110, 1, 7),
(125, 1, 8),
(113, 1, 9),
(115, 1, 10),
(117, 1, 11),
(102, 1, 12),
(104, 1, 13),
(107, 1, 14),
(119, 1, 15),
(109, 1, 18),
(111, 1, 19),
(121, 1, 20),
(124, 1, 21),
(112, 1, 22),
(114, 1, 23),
(116, 1, 24),
(118, 1, 25),
(120, 1, 26),
(122, 1, 27),
(123, 1, 28),
(126, 1, 29),
(127, 1, 30),
(128, 1, 31),
(129, 1, 32),
(62, 2, 1),
(32, 2, 6),
(52, 2, 7),
(56, 2, 9),
(33, 2, 10),
(48, 2, 13),
(51, 2, 18),
(50, 2, 19),
(54, 2, 20),
(38, 3, 6),
(36, 3, 7),
(37, 3, 8),
(57, 3, 9),
(47, 4, 1),
(34, 4, 6),
(42, 4, 9),
(35, 4, 10),
(49, 5, 1),
(39, 5, 3),
(40, 5, 5),
(41, 5, 6),
(71, 32, 1),
(72, 32, 6),
(73, 32, 7),
(74, 32, 15),
(65, 33, 1),
(66, 33, 6),
(67, 33, 15),
(68, 34, 1),
(69, 34, 6),
(70, 34, 15);

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `ventas`
--

CREATE TABLE `ventas` (
  `id` int(11) NOT NULL,
  `fecha` datetime DEFAULT current_timestamp(),
  `mesa_id` int(11) DEFAULT NULL,
  `repartidor_id` int(11) DEFAULT NULL,
  `tipo_entrega` enum('mesa','domicilio','rapido') DEFAULT 'mesa',
  `usuario_id` int(11) DEFAULT NULL,
  `total` decimal(10,2) DEFAULT 0.00,
  `estatus` enum('activa','cerrada','cancelada') DEFAULT 'activa',
  `entregado` tinyint(1) DEFAULT 0,
  `estado_entrega` enum('pendiente','en_camino','entregado') DEFAULT 'pendiente',
  `fecha_asignacion` datetime DEFAULT NULL,
  `fecha_inicio` datetime DEFAULT NULL,
  `fecha_entrega` datetime DEFAULT NULL,
  `seudonimo_entrega` varchar(100) DEFAULT NULL,
  `foto_entrega` varchar(255) DEFAULT NULL,
  `corte_id` int(11) DEFAULT NULL,
  `cajero_id` int(11) DEFAULT NULL,
  `observacion` text DEFAULT NULL,
  `sede_id` int(11) DEFAULT NULL,
  `propina_efectivo` decimal(10,2) NOT NULL,
  `propina_cheque` decimal(10,2) NOT NULL,
  `propina_tarjeta` decimal(10,2) NOT NULL,
  `promocion_id` int(11) DEFAULT NULL,
  `promocion_descuento` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `ventas`
--

INSERT INTO `ventas` (`id`, `fecha`, `mesa_id`, `repartidor_id`, `tipo_entrega`, `usuario_id`, `total`, `estatus`, `entregado`, `estado_entrega`, `fecha_asignacion`, `fecha_inicio`, `fecha_entrega`, `seudonimo_entrega`, `foto_entrega`, `corte_id`, `cajero_id`, `observacion`, `sede_id`, `propina_efectivo`, `propina_cheque`, `propina_tarjeta`, `promocion_id`, `promocion_descuento`) VALUES
(344, '2025-09-13 19:55:25', NULL, NULL, 'rapido', 1, 347.17, 'cerrada', 0, 'pendiente', NULL, NULL, NULL, NULL, NULL, 82, NULL, NULL, 1, 0.00, 0.00, 0.00, NULL, NULL),
(345, '2025-09-13 20:01:03', NULL, 4, 'domicilio', 35, 379.00, 'cerrada', 0, 'pendiente', '2025-09-13 20:01:03', NULL, NULL, NULL, NULL, 82, 1, '', 1, 0.00, 0.00, 0.00, 4, 105.00);

--
-- Disparadores `ventas`
--
DELIMITER $$
CREATE TRIGGER `trg_log_cancel_venta` AFTER UPDATE ON `ventas` FOR EACH ROW BEGIN
  IF COALESCE(@ARCHIVE_MODE,0)=0 AND OLD.estatus <> 'cancelada' AND NEW.estatus = 'cancelada' THEN
    INSERT INTO log_cancelaciones (tipo, venta_id, usuario_id, motivo, total_anterior, fecha)
    VALUES ('venta', NEW.id, NEW.usuario_id, NEW.observacion, OLD.total, NOW());
  END IF;
END
$$
DELIMITER ;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `venta_detalles`
--

CREATE TABLE `venta_detalles` (
  `id` int(11) NOT NULL,
  `venta_id` int(11) NOT NULL,
  `producto_id` int(11) NOT NULL,
  `cantidad` int(11) NOT NULL DEFAULT 1,
  `precio_unitario` decimal(10,2) NOT NULL,
  `subtotal` decimal(10,2) GENERATED ALWAYS AS (`cantidad` * `precio_unitario`) STORED,
  `insumos_descargados` tinyint(1) DEFAULT 0,
  `created_at` datetime DEFAULT current_timestamp(),
  `entregado_hr` datetime DEFAULT NULL,
  `estado_producto` enum('pendiente','en_preparacion','listo','entregado') DEFAULT 'pendiente',
  `observaciones` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `venta_detalles`
--

INSERT INTO `venta_detalles` (`id`, `venta_id`, `producto_id`, `cantidad`, `precio_unitario`, `insumos_descargados`, `created_at`, `entregado_hr`, `estado_producto`, `observaciones`) VALUES
(1363, 344, 77, 1, 10.00, 1, '2025-09-13 19:55:25', '2025-09-13 20:24:31', 'entregado', NULL),
(1364, 344, 14, 1, 115.00, 1, '2025-09-13 19:55:25', '2025-09-13 20:52:58', 'entregado', NULL),
(1365, 344, 105, 1, 135.00, 1, '2025-09-13 19:55:25', '2025-09-13 20:24:33', 'entregado', NULL),
(1366, 344, 124, 1, 35.00, 1, '2025-09-13 19:55:25', '2025-09-13 20:52:59', 'entregado', NULL),
(1367, 344, 74, 1, 35.00, 1, '2025-09-13 19:55:25', '2025-09-13 20:24:32', 'entregado', NULL),
(1368, 344, 9000, 1, 17.17, 0, '2025-09-13 19:55:25', '2025-09-13 19:55:25', 'entregado', NULL),
(1369, 345, 15, 1, 115.00, 1, '2025-09-13 20:01:03', '2025-09-13 20:53:00', 'entregado', NULL),
(1370, 345, 13, 1, 109.00, 1, '2025-09-13 20:01:03', '2025-09-13 20:53:02', 'entregado', NULL),
(1371, 345, 16, 1, 105.00, 1, '2025-09-13 20:01:03', '2025-09-13 20:53:05', 'entregado', NULL),
(1372, 345, 9001, 1, 50.00, 0, '2025-09-13 20:01:03', NULL, 'entregado', NULL);

--
-- Disparadores `venta_detalles`
--
DELIMITER $$
CREATE TRIGGER `tr_log_estado_producto` AFTER UPDATE ON `venta_detalles` FOR EACH ROW BEGIN
  IF NOT (OLD.estado_producto <=> NEW.estado_producto) THEN
    INSERT INTO venta_detalles_log (venta_detalle_id, estado_anterior, estado_nuevo)
    VALUES (OLD.id, OLD.estado_producto, NEW.estado_producto);
  END IF;
END
$$
DELIMITER ;
DELIMITER $$
CREATE TRIGGER `trg_log_cancel_detalle` BEFORE DELETE ON `venta_detalles` FOR EACH ROW BEGIN
  IF COALESCE(@ARCHIVE_MODE,0)=0 THEN
    INSERT INTO log_cancelaciones
      (tipo, venta_id, venta_detalle_id, usuario_id, motivo, subtotal_detalle, fecha)
    VALUES
      ('detalle', OLD.venta_id, OLD.id, @usuario_id, COALESCE(@motivo_cancelacion,'Eliminación de producto'), OLD.subtotal, NOW());
  END IF;
END
$$
DELIMITER ;
DELIMITER $$
CREATE TRIGGER `trg_respaldo_venta_detalles` BEFORE DELETE ON `venta_detalles` FOR EACH ROW BEGIN
  IF COALESCE(@ARCHIVE_MODE,0)=0 THEN
    INSERT INTO venta_detalles_cancelados (
      venta_detalle_id_original, venta_id, producto_id, cantidad, precio_unitario,
      insumos_descargados, created_at, entregado_hr, estado_producto, observaciones,
      cancelado_por, motivo
    ) VALUES (
      OLD.id, OLD.venta_id, OLD.producto_id, OLD.cantidad, OLD.precio_unitario,
      OLD.insumos_descargados, OLD.created_at, OLD.entregado_hr, OLD.estado_producto, OLD.observaciones,
      @usuario_id, @motivo_cancelacion
    );
  END IF;
END
$$
DELIMITER ;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `venta_detalles_cancelados`
--

CREATE TABLE `venta_detalles_cancelados` (
  `id` int(11) NOT NULL,
  `venta_detalle_id_original` int(11) NOT NULL,
  `venta_id` int(11) NOT NULL,
  `producto_id` int(11) NOT NULL,
  `cantidad` int(11) NOT NULL,
  `precio_unitario` decimal(10,2) NOT NULL,
  `insumos_descargados` tinyint(1) DEFAULT 0,
  `created_at` datetime DEFAULT NULL,
  `entregado_hr` datetime DEFAULT NULL,
  `estado_producto` enum('pendiente','en_preparacion','listo','entregado') DEFAULT 'pendiente',
  `observaciones` text DEFAULT NULL,
  `cancelado_por` int(11) DEFAULT NULL,
  `fecha_cancelacion` datetime DEFAULT current_timestamp(),
  `motivo` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `venta_detalles_log`
--

CREATE TABLE `venta_detalles_log` (
  `id` int(11) NOT NULL,
  `venta_detalle_id` int(11) NOT NULL,
  `estado_anterior` enum('pendiente','en_preparacion','listo','entregado') DEFAULT NULL,
  `estado_nuevo` enum('pendiente','en_preparacion','listo','entregado') NOT NULL,
  `cambiado_en` datetime DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Volcado de datos para la tabla `venta_detalles_log`
--

INSERT INTO `venta_detalles_log` (`id`, `venta_detalle_id`, `estado_anterior`, `estado_nuevo`, `cambiado_en`) VALUES
(560, 1368, 'pendiente', 'entregado', '2025-09-13 19:55:25'),
(561, 1372, 'pendiente', 'entregado', '2025-09-13 20:01:03'),
(562, 1363, 'pendiente', 'en_preparacion', '2025-09-13 20:23:43'),
(563, 1365, 'pendiente', 'en_preparacion', '2025-09-13 20:23:44'),
(564, 1364, 'pendiente', 'en_preparacion', '2025-09-13 20:23:45'),
(565, 1367, 'pendiente', 'en_preparacion', '2025-09-13 20:23:47'),
(566, 1366, 'pendiente', 'en_preparacion', '2025-09-13 20:23:48'),
(567, 1363, 'en_preparacion', 'listo', '2025-09-13 20:23:48'),
(568, 1365, 'en_preparacion', 'listo', '2025-09-13 20:23:49'),
(569, 1367, 'en_preparacion', 'listo', '2025-09-13 20:23:50'),
(570, 1369, 'pendiente', 'en_preparacion', '2025-09-13 20:23:51'),
(571, 1364, 'en_preparacion', 'listo', '2025-09-13 20:24:29'),
(572, 1366, 'en_preparacion', 'listo', '2025-09-13 20:24:30'),
(573, 1369, 'en_preparacion', 'listo', '2025-09-13 20:24:31'),
(574, 1363, 'listo', 'entregado', '2025-09-13 20:24:31'),
(575, 1367, 'listo', 'entregado', '2025-09-13 20:24:32'),
(576, 1365, 'listo', 'entregado', '2025-09-13 20:24:33'),
(577, 1364, 'listo', 'entregado', '2025-09-13 20:52:58'),
(578, 1366, 'listo', 'entregado', '2025-09-13 20:52:59'),
(579, 1369, 'listo', 'entregado', '2025-09-13 20:53:00'),
(580, 1370, 'pendiente', 'en_preparacion', '2025-09-13 20:53:01'),
(581, 1370, 'en_preparacion', 'listo', '2025-09-13 20:53:02'),
(582, 1370, 'listo', 'entregado', '2025-09-13 20:53:02'),
(583, 1371, 'pendiente', 'en_preparacion', '2025-09-13 20:53:03'),
(584, 1371, 'en_preparacion', 'listo', '2025-09-13 20:53:04'),
(585, 1371, 'listo', 'entregado', '2025-09-13 20:53:05');

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vista_facturas`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vista_facturas` (
`factura_id` int(11)
,`folio` varchar(50)
,`uuid` varchar(64)
,`subtotal` decimal(10,2)
,`impuestos` decimal(10,2)
,`total` decimal(10,2)
,`fecha_emision` datetime
,`estado` enum('generada','cancelada')
,`ticket_id` int(11)
,`ticket_folio` int(11)
,`ticket_total` decimal(10,2)
,`cliente_id` int(11)
,`rfc` varchar(20)
,`razon_social` varchar(200)
,`correo` varchar(150)
,`telefono` varchar(30)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vista_factura_detalles`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vista_factura_detalles` (
`id` int(11)
,`factura_id` int(11)
,`ticket_detalle_id` int(11)
,`producto_id` int(11)
,`descripcion` varchar(255)
,`cantidad` int(11)
,`precio_unitario` decimal(10,2)
,`importe` decimal(10,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vista_productos_mas_vendidos`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vista_productos_mas_vendidos` (
`producto_id` int(11)
,`nombre` varchar(100)
,`total_vendidos` decimal(32,0)
,`total_ingresos` decimal(42,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vista_resumen_cortes`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vista_resumen_cortes` (
`corte_id` int(11)
,`usuario_id` int(11)
,`fecha_inicio` datetime
,`fecha_fin` datetime
,`fondo_inicial` decimal(10,2)
,`total_corte` decimal(10,2)
,`total_ventas` decimal(32,2)
,`total_propinas` decimal(12,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vista_resumen_pagos`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vista_resumen_pagos` (
`corte_id` int(11)
,`tipo_pago` enum('efectivo','boucher','cheque')
,`total_productos` decimal(35,2)
,`total_propinas` decimal(12,2)
,`total_con_propina` decimal(32,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vista_ventas_diarias`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vista_ventas_diarias` (
`fecha` date
,`cantidad_ventas` bigint(21)
,`total_productos` decimal(35,2)
,`total_propinas` decimal(12,2)
,`total_general` decimal(32,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_consumo_insumos`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_consumo_insumos` (
`venta_id` int(11)
,`insumo_id` int(11)
,`insumo` varchar(100)
,`unidad` varchar(20)
,`total_consumido` decimal(42,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_corte_resumen`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_corte_resumen` (
`corte_id` int(11)
,`cajero` varchar(100)
,`fecha_inicio` datetime
,`fecha_fin` datetime
,`total` decimal(10,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_kpi_producto`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_kpi_producto` (
`producto_id` int(11)
,`total_ordenes` bigint(21)
,`avg_pendiente_a_preparacion` decimal(21,0)
,`avg_preparacion_a_listo` decimal(21,0)
,`avg_listo_a_entregado` decimal(21,0)
,`avg_total_servicio` decimal(21,0)
,`max_total_servicio` bigint(21)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_kpi_venta`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_kpi_venta` (
`venta_id` int(11)
,`productos` bigint(21)
,`avg_servicio_por_producto` decimal(21,0)
,`max_servicio_producto` bigint(21)
,`total_servicio_venta` bigint(21)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_productos_recetas_agrupado`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_productos_recetas_agrupado` (
`producto_id` int(11)
,`producto` varchar(100)
,`receta` mediumtext
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_tickets_con_descuentos`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_tickets_con_descuentos` (
`ticket_id` int(11)
,`venta_id` int(11)
,`total_bruto` decimal(10,2)
,`descuento_total` decimal(10,2)
,`total_esperado` decimal(11,2)
,`descuento_cortesias` decimal(32,2)
,`porcentaje_aplicado` decimal(5,2)
,`descuento_porcentaje` decimal(32,2)
,`descuento_monto_fijo` decimal(32,2)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_tiempos_producto`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_tiempos_producto` (
`venta_detalle_id` int(11)
,`venta_id` int(11)
,`producto_id` int(11)
,`ts_pendiente` datetime
,`ts_preparacion` datetime
,`ts_listo` datetime
,`ts_entregado` datetime
,`segs_pendiente_a_preparacion` bigint(21)
,`segs_preparacion_a_listo` bigint(21)
,`segs_listo_a_entregado` bigint(21)
,`segs_total_servicio` bigint(21)
);

-- --------------------------------------------------------

--
-- Estructura Stand-in para la vista `vw_ventas_detalladas`
-- (Véase abajo para la vista actual)
--
CREATE TABLE `vw_ventas_detalladas` (
`venta_id` int(11)
,`fecha` datetime
,`total` decimal(10,2)
,`estatus` enum('activa','cerrada','cancelada')
,`usuario` varchar(100)
,`mesa` varchar(50)
,`repartidor` varchar(100)
);

-- --------------------------------------------------------

--
-- Estructura para la vista `vista_facturas`
--
DROP TABLE IF EXISTS `vista_facturas`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vista_facturas`  AS SELECT `f`.`id` AS `factura_id`, `f`.`folio` AS `folio`, `f`.`uuid` AS `uuid`, `f`.`subtotal` AS `subtotal`, `f`.`impuestos` AS `impuestos`, `f`.`total` AS `total`, `f`.`fecha_emision` AS `fecha_emision`, `f`.`estado` AS `estado`, `f`.`ticket_id` AS `ticket_id`, `t`.`folio` AS `ticket_folio`, `t`.`total` AS `ticket_total`, `c`.`id` AS `cliente_id`, `c`.`rfc` AS `rfc`, `c`.`razon_social` AS `razon_social`, `c`.`correo` AS `correo`, `c`.`telefono` AS `telefono` FROM ((`facturas` `f` join `tickets` `t` on(`t`.`id` = `f`.`ticket_id`)) join `clientes_facturacion` `c` on(`c`.`id` = `f`.`cliente_id`)) ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vista_factura_detalles`
--
DROP TABLE IF EXISTS `vista_factura_detalles`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vista_factura_detalles`  AS SELECT `fd`.`id` AS `id`, `fd`.`factura_id` AS `factura_id`, `fd`.`ticket_detalle_id` AS `ticket_detalle_id`, `fd`.`producto_id` AS `producto_id`, coalesce(`fd`.`descripcion`,`p`.`nombre`) AS `descripcion`, `fd`.`cantidad` AS `cantidad`, `fd`.`precio_unitario` AS `precio_unitario`, `fd`.`importe` AS `importe` FROM (`factura_detalles` `fd` left join `productos` `p` on(`p`.`id` = `fd`.`producto_id`)) ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vista_productos_mas_vendidos`
--
DROP TABLE IF EXISTS `vista_productos_mas_vendidos`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vista_productos_mas_vendidos`  AS SELECT `vd`.`producto_id` AS `producto_id`, `p`.`nombre` AS `nombre`, sum(`vd`.`cantidad`) AS `total_vendidos`, sum(`vd`.`cantidad` * `vd`.`precio_unitario`) AS `total_ingresos` FROM (`venta_detalles` `vd` join `productos` `p` on(`vd`.`producto_id` = `p`.`id`)) GROUP BY `vd`.`producto_id`, `p`.`nombre` ORDER BY sum(`vd`.`cantidad`) DESC ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vista_resumen_cortes`
--
DROP TABLE IF EXISTS `vista_resumen_cortes`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vista_resumen_cortes`  AS SELECT `c`.`id` AS `corte_id`, `c`.`usuario_id` AS `usuario_id`, `c`.`fecha_inicio` AS `fecha_inicio`, `c`.`fecha_fin` AS `fecha_fin`, `c`.`fondo_inicial` AS `fondo_inicial`, `c`.`total` AS `total_corte`, coalesce(sum(`t`.`total`),0) AS `total_ventas`, coalesce(`v`.`propina_efectivo` + `v`.`propina_cheque` + `v`.`propina_tarjeta`,0) AS `total_propinas` FROM ((`corte_caja` `c` left join `ventas` `v` on(`v`.`corte_id` = `c`.`id` and `v`.`estatus` = 'cerrada')) left join `tickets` `t` on(`t`.`venta_id` = `v`.`id`)) GROUP BY `c`.`id` ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vista_resumen_pagos`
--
DROP TABLE IF EXISTS `vista_resumen_pagos`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vista_resumen_pagos`  AS SELECT `v`.`corte_id` AS `corte_id`, `t`.`tipo_pago` AS `tipo_pago`, sum(`t`.`total` - (`v`.`propina_efectivo` + `v`.`propina_cheque` + `v`.`propina_tarjeta`)) AS `total_productos`, `v`.`propina_efectivo`+ `v`.`propina_cheque` + `v`.`propina_tarjeta` AS `total_propinas`, sum(`t`.`total`) AS `total_con_propina` FROM (`tickets` `t` join `ventas` `v` on(`t`.`venta_id` = `v`.`id`)) WHERE `v`.`estatus` = 'cerrada' GROUP BY `v`.`corte_id`, `t`.`tipo_pago` ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vista_ventas_diarias`
--
DROP TABLE IF EXISTS `vista_ventas_diarias`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vista_ventas_diarias`  AS SELECT cast(`t`.`fecha` as date) AS `fecha`, count(0) AS `cantidad_ventas`, sum(`t`.`total` - (`v`.`propina_efectivo` + `v`.`propina_cheque` + `v`.`propina_tarjeta`)) AS `total_productos`, `v`.`propina_efectivo`+ `v`.`propina_cheque` + `v`.`propina_tarjeta` AS `total_propinas`, sum(`t`.`total`) AS `total_general` FROM (`tickets` `t` join `ventas` `v` on(`t`.`venta_id` = `v`.`id`)) WHERE `v`.`estatus` = 'cerrada' GROUP BY cast(`t`.`fecha` as date) ORDER BY cast(`t`.`fecha` as date) DESC ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_consumo_insumos`
--
DROP TABLE IF EXISTS `vw_consumo_insumos`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_consumo_insumos`  AS SELECT `vd`.`venta_id` AS `venta_id`, `r`.`insumo_id` AS `insumo_id`, `i`.`nombre` AS `insumo`, `i`.`unidad` AS `unidad`, sum(`r`.`cantidad` * `vd`.`cantidad`) AS `total_consumido` FROM ((`venta_detalles` `vd` join `recetas` `r` on(`vd`.`producto_id` = `r`.`producto_id`)) join `insumos` `i` on(`r`.`insumo_id` = `i`.`id`)) GROUP BY `vd`.`venta_id`, `r`.`insumo_id` ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_corte_resumen`
--
DROP TABLE IF EXISTS `vw_corte_resumen`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_corte_resumen`  AS SELECT `c`.`id` AS `corte_id`, `u`.`nombre` AS `cajero`, `c`.`fecha_inicio` AS `fecha_inicio`, `c`.`fecha_fin` AS `fecha_fin`, `c`.`total` AS `total` FROM (`corte_caja` `c` join `usuarios` `u` on(`c`.`usuario_id` = `u`.`id`)) ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_kpi_producto`
--
DROP TABLE IF EXISTS `vw_kpi_producto`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_kpi_producto`  AS SELECT `vp`.`producto_id` AS `producto_id`, count(0) AS `total_ordenes`, round(avg(`vp`.`segs_pendiente_a_preparacion`),0) AS `avg_pendiente_a_preparacion`, round(avg(`vp`.`segs_preparacion_a_listo`),0) AS `avg_preparacion_a_listo`, round(avg(`vp`.`segs_listo_a_entregado`),0) AS `avg_listo_a_entregado`, round(avg(`vp`.`segs_total_servicio`),0) AS `avg_total_servicio`, max(`vp`.`segs_total_servicio`) AS `max_total_servicio` FROM `vw_tiempos_producto` AS `vp` GROUP BY `vp`.`producto_id` ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_kpi_venta`
--
DROP TABLE IF EXISTS `vw_kpi_venta`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_kpi_venta`  AS SELECT `vp`.`venta_id` AS `venta_id`, count(distinct `vp`.`producto_id`) AS `productos`, round(avg(`vp`.`segs_total_servicio`),0) AS `avg_servicio_por_producto`, max(`vp`.`segs_total_servicio`) AS `max_servicio_producto`, timestampdiff(SECOND,min(`vp`.`ts_pendiente`),max(`vp`.`ts_entregado`)) AS `total_servicio_venta` FROM `vw_tiempos_producto` AS `vp` GROUP BY `vp`.`venta_id` ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_productos_recetas_agrupado`
--
DROP TABLE IF EXISTS `vw_productos_recetas_agrupado`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_productos_recetas_agrupado`  AS SELECT `p`.`id` AS `producto_id`, `p`.`nombre` AS `producto`, group_concat(concat(`i`.`nombre`,' (',`r`.`cantidad`,' ',`i`.`unidad`,')') order by `i`.`nombre` ASC separator ' | ') AS `receta` FROM ((`recetas` `r` join `productos` `p` on(`p`.`id` = `r`.`producto_id`)) join `insumos` `i` on(`i`.`id` = `r`.`insumo_id`)) GROUP BY `p`.`id`, `p`.`nombre` ORDER BY `p`.`nombre` ASC ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_tickets_con_descuentos`
--
DROP TABLE IF EXISTS `vw_tickets_con_descuentos`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_tickets_con_descuentos`  AS SELECT `t`.`id` AS `ticket_id`, `t`.`venta_id` AS `venta_id`, `t`.`total` AS `total_bruto`, `t`.`descuento` AS `descuento_total`, `t`.`total`- `t`.`descuento` AS `total_esperado`, sum(case when `td`.`tipo` = 'cortesia' then `td`.`monto` else 0 end) AS `descuento_cortesias`, max(case when `td`.`tipo` = 'porcentaje' then `td`.`porcentaje` end) AS `porcentaje_aplicado`, sum(case when `td`.`tipo` = 'porcentaje' then `td`.`monto` else 0 end) AS `descuento_porcentaje`, sum(case when `td`.`tipo` = 'monto_fijo' then `td`.`monto` else 0 end) AS `descuento_monto_fijo` FROM (`tickets` `t` left join `ticket_descuentos` `td` on(`td`.`ticket_id` = `t`.`id`)) GROUP BY `t`.`id`, `t`.`venta_id`, `t`.`total`, `t`.`descuento` ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_tiempos_producto`
--
DROP TABLE IF EXISTS `vw_tiempos_producto`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_tiempos_producto`  AS SELECT `vd`.`id` AS `venta_detalle_id`, `vd`.`venta_id` AS `venta_id`, `vd`.`producto_id` AS `producto_id`, coalesce(min(case when `vdl`.`estado_nuevo` = 'pendiente' then `vdl`.`cambiado_en` end),`vd`.`created_at`) AS `ts_pendiente`, min(case when `vdl`.`estado_nuevo` = 'en_preparacion' then `vdl`.`cambiado_en` end) AS `ts_preparacion`, min(case when `vdl`.`estado_nuevo` = 'listo' then `vdl`.`cambiado_en` end) AS `ts_listo`, min(case when `vdl`.`estado_nuevo` = 'entregado' then `vdl`.`cambiado_en` end) AS `ts_entregado`, timestampdiff(SECOND,coalesce(min(case when `vdl`.`estado_nuevo` = 'pendiente' then `vdl`.`cambiado_en` end),`vd`.`created_at`),min(case when `vdl`.`estado_nuevo` = 'en_preparacion' then `vdl`.`cambiado_en` end)) AS `segs_pendiente_a_preparacion`, timestampdiff(SECOND,min(case when `vdl`.`estado_nuevo` = 'en_preparacion' then `vdl`.`cambiado_en` end),min(case when `vdl`.`estado_nuevo` = 'listo' then `vdl`.`cambiado_en` end)) AS `segs_preparacion_a_listo`, timestampdiff(SECOND,min(case when `vdl`.`estado_nuevo` = 'listo' then `vdl`.`cambiado_en` end),min(case when `vdl`.`estado_nuevo` = 'entregado' then `vdl`.`cambiado_en` end)) AS `segs_listo_a_entregado`, timestampdiff(SECOND,coalesce(min(case when `vdl`.`estado_nuevo` = 'pendiente' then `vdl`.`cambiado_en` end),`vd`.`created_at`),min(case when `vdl`.`estado_nuevo` = 'entregado' then `vdl`.`cambiado_en` end)) AS `segs_total_servicio` FROM (`venta_detalles` `vd` left join `venta_detalles_log` `vdl` on(`vdl`.`venta_detalle_id` = `vd`.`id`)) GROUP BY `vd`.`id`, `vd`.`venta_id`, `vd`.`producto_id` ;

-- --------------------------------------------------------

--
-- Estructura para la vista `vw_ventas_detalladas`
--
DROP TABLE IF EXISTS `vw_ventas_detalladas`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `vw_ventas_detalladas`  AS SELECT `v`.`id` AS `venta_id`, `v`.`fecha` AS `fecha`, `v`.`total` AS `total`, `v`.`estatus` AS `estatus`, `u`.`nombre` AS `usuario`, `m`.`nombre` AS `mesa`, `r`.`nombre` AS `repartidor` FROM (((`ventas` `v` left join `usuarios` `u` on(`v`.`usuario_id` = `u`.`id`)) left join `mesas` `m` on(`v`.`mesa_id` = `m`.`id`)) left join `repartidores` `r` on(`v`.`repartidor_id` = `r`.`id`)) ;

--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `alineacion`
--
ALTER TABLE `alineacion`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `nombre` (`nombre`);

--
-- Indices de la tabla `catalogo_areas`
--
ALTER TABLE `catalogo_areas`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `catalogo_bancos`
--
ALTER TABLE `catalogo_bancos`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `catalogo_categorias`
--
ALTER TABLE `catalogo_categorias`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `catalogo_denominaciones`
--
ALTER TABLE `catalogo_denominaciones`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `catalogo_folios`
--
ALTER TABLE `catalogo_folios`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `catalogo_promos`
--
ALTER TABLE `catalogo_promos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_cp_activo_visible` (`activo`,`visible_en_ticket`),
  ADD KEY `idx_cp_tipo` (`tipo`);

--
-- Indices de la tabla `catalogo_tarjetas`
--
ALTER TABLE `catalogo_tarjetas`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `clientes_facturacion`
--
ALTER TABLE `clientes_facturacion`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `rfc` (`rfc`);

--
-- Indices de la tabla `conekta_events`
--
ALTER TABLE `conekta_events`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_ref` (`reference`),
  ADD KEY `idx_evt` (`event_type`);

--
-- Indices de la tabla `conekta_payments`
--
ALTER TABLE `conekta_payments`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uk_reference` (`reference`),
  ADD KEY `idx_venta` (`venta_id`),
  ADD KEY `idx_order` (`conekta_order_id`);

--
-- Indices de la tabla `cortes_almacen`
--
ALTER TABLE `cortes_almacen`
  ADD PRIMARY KEY (`id`),
  ADD KEY `usuario_abre_id` (`usuario_abre_id`),
  ADD KEY `usuario_cierra_id` (`usuario_cierra_id`);

--
-- Indices de la tabla `cortes_almacen_detalle`
--
ALTER TABLE `cortes_almacen_detalle`
  ADD PRIMARY KEY (`id`),
  ADD KEY `corte_id` (`corte_id`),
  ADD KEY `insumo_id` (`insumo_id`);

--
-- Indices de la tabla `corte_caja`
--
ALTER TABLE `corte_caja`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `id_padre` (`id`),
  ADD KEY `usuario_id` (`usuario_id`);

--
-- Indices de la tabla `corte_caja_historial`
--
ALTER TABLE `corte_caja_historial`
  ADD PRIMARY KEY (`id`),
  ADD KEY `corte_id` (`corte_id`),
  ADD KEY `usuario_id` (`usuario_id`);

--
-- Indices de la tabla `desglose_corte`
--
ALTER TABLE `desglose_corte`
  ADD PRIMARY KEY (`id`),
  ADD KEY `corte_id` (`corte_id`),
  ADD KEY `fk_denominacion` (`denominacion_id`);

--
-- Indices de la tabla `entradas_detalle`
--
ALTER TABLE `entradas_detalle`
  ADD PRIMARY KEY (`id`),
  ADD KEY `entrada_id` (`entrada_id`),
  ADD KEY `fk_entrada_detalle_insumo` (`insumo_id`);

--
-- Indices de la tabla `entradas_insumo`
--
ALTER TABLE `entradas_insumo`
  ADD PRIMARY KEY (`id`),
  ADD KEY `proveedor_id` (`proveedor_id`),
  ADD KEY `fk_entrada_usuario` (`usuario_id`);

--
-- Indices de la tabla `facturas`
--
ALTER TABLE `facturas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_fact_ticket` (`ticket_id`),
  ADD KEY `idx_fact_cliente` (`cliente_id`);

--
-- Indices de la tabla `factura_detalles`
--
ALTER TABLE `factura_detalles`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uq_fd_ticketdet` (`ticket_detalle_id`),
  ADD KEY `idx_fd_fact` (`factura_id`);

--
-- Indices de la tabla `factura_tickets`
--
ALTER TABLE `factura_tickets`
  ADD PRIMARY KEY (`factura_id`,`ticket_id`),
  ADD UNIQUE KEY `uq_ticket_unico` (`ticket_id`);

--
-- Indices de la tabla `fondo`
--
ALTER TABLE `fondo`
  ADD PRIMARY KEY (`usuario_id`);

--
-- Indices de la tabla `horarios`
--
ALTER TABLE `horarios`
  ADD PRIMARY KEY (`id`),
  ADD KEY `fk_horario_serie` (`serie_id`);

--
-- Indices de la tabla `insumos`
--
ALTER TABLE `insumos`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `logs_accion`
--
ALTER TABLE `logs_accion`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `log_asignaciones_mesas`
--
ALTER TABLE `log_asignaciones_mesas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `mesa_id` (`mesa_id`),
  ADD KEY `mesero_anterior_id` (`mesero_anterior_id`),
  ADD KEY `mesero_nuevo_id` (`mesero_nuevo_id`),
  ADD KEY `usuario_que_asigna_id` (`usuario_que_asigna_id`);

--
-- Indices de la tabla `log_cancelaciones`
--
ALTER TABLE `log_cancelaciones`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `log_mesas`
--
ALTER TABLE `log_mesas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `mesa_id` (`mesa_id`),
  ADD KEY `venta_id` (`venta_id`),
  ADD KEY `usuario_id` (`usuario_id`);

--
-- Indices de la tabla `menu_dia`
--
ALTER TABLE `menu_dia`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `mesas`
--
ALTER TABLE `mesas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `usuario_id` (`usuario_id`),
  ADD KEY `fk_mesa_area` (`area_id`),
  ADD KEY `fk_mesas_alineacion` (`alineacion_id`);

--
-- Indices de la tabla `movimientos_caja`
--
ALTER TABLE `movimientos_caja`
  ADD PRIMARY KEY (`id`),
  ADD KEY `corte_id` (`corte_id`),
  ADD KEY `usuario_id` (`usuario_id`);

--
-- Indices de la tabla `movimientos_insumos`
--
ALTER TABLE `movimientos_insumos`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `ofertas_dia`
--
ALTER TABLE `ofertas_dia`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `productos`
--
ALTER TABLE `productos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `fk_productos_categoria` (`categoria_id`);

--
-- Indices de la tabla `proveedores`
--
ALTER TABLE `proveedores`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `qrs_insumo`
--
ALTER TABLE `qrs_insumo`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `recetas`
--
ALTER TABLE `recetas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `producto_id` (`producto_id`),
  ADD KEY `insumo_id` (`insumo_id`);

--
-- Indices de la tabla `repartidores`
--
ALTER TABLE `repartidores`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `rutas`
--
ALTER TABLE `rutas`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `sedes`
--
ALTER TABLE `sedes`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `tickets`
--
ALTER TABLE `tickets`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `ux_tickets_serie_folio` (`serie_id`,`folio`),
  ADD KEY `venta_id` (`venta_id`),
  ADD KEY `usuario_id` (`usuario_id`),
  ADD KEY `fk_tickets_sede` (`sede_id`),
  ADD KEY `fk_ticket_tarjeta_marca` (`tarjeta_marca_id`),
  ADD KEY `fk_ticket_tarjeta_banco` (`tarjeta_banco_id`),
  ADD KEY `fk_ticket_cheque_banco` (`cheque_banco_id`);

--
-- Indices de la tabla `ticket_descuentos`
--
ALTER TABLE `ticket_descuentos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `ticket_id` (`ticket_id`),
  ADD KEY `venta_detalle_id` (`venta_detalle_id`);

--
-- Indices de la tabla `ticket_detalles`
--
ALTER TABLE `ticket_detalles`
  ADD PRIMARY KEY (`id`),
  ADD KEY `ticket_id` (`ticket_id`),
  ADD KEY `producto_id` (`producto_id`);

--
-- Indices de la tabla `usuarios`
--
ALTER TABLE `usuarios`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `usuario` (`usuario`);

--
-- Indices de la tabla `usuario_ruta`
--
ALTER TABLE `usuario_ruta`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `usuario_id` (`usuario_id`,`ruta_id`),
  ADD KEY `ruta_id` (`ruta_id`);

--
-- Indices de la tabla `ventas`
--
ALTER TABLE `ventas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `mesa_id` (`mesa_id`),
  ADD KEY `repartidor_id` (`repartidor_id`),
  ADD KEY `usuario_id` (`usuario_id`),
  ADD KEY `fk_corte` (`corte_id`),
  ADD KEY `cajero_id` (`cajero_id`),
  ADD KEY `fk_ventas_sede` (`sede_id`);

--
-- Indices de la tabla `venta_detalles`
--
ALTER TABLE `venta_detalles`
  ADD PRIMARY KEY (`id`),
  ADD KEY `venta_id` (`venta_id`),
  ADD KEY `producto_id` (`producto_id`);

--
-- Indices de la tabla `venta_detalles_cancelados`
--
ALTER TABLE `venta_detalles_cancelados`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `venta_detalles_log`
--
ALTER TABLE `venta_detalles_log`
  ADD PRIMARY KEY (`id`),
  ADD KEY `venta_detalle_id` (`venta_detalle_id`);

--
-- AUTO_INCREMENT de las tablas volcadas
--

--
-- AUTO_INCREMENT de la tabla `alineacion`
--
ALTER TABLE `alineacion`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;

--
-- AUTO_INCREMENT de la tabla `catalogo_areas`
--
ALTER TABLE `catalogo_areas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT de la tabla `catalogo_bancos`
--
ALTER TABLE `catalogo_bancos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=9;

--
-- AUTO_INCREMENT de la tabla `catalogo_categorias`
--
ALTER TABLE `catalogo_categorias`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=14;

--
-- AUTO_INCREMENT de la tabla `catalogo_denominaciones`
--
ALTER TABLE `catalogo_denominaciones`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=14;

--
-- AUTO_INCREMENT de la tabla `catalogo_folios`
--
ALTER TABLE `catalogo_folios`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT de la tabla `catalogo_promos`
--
ALTER TABLE `catalogo_promos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;

--
-- AUTO_INCREMENT de la tabla `catalogo_tarjetas`
--
ALTER TABLE `catalogo_tarjetas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;

--
-- AUTO_INCREMENT de la tabla `clientes_facturacion`
--
ALTER TABLE `clientes_facturacion`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT de la tabla `conekta_events`
--
ALTER TABLE `conekta_events`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=57;

--
-- AUTO_INCREMENT de la tabla `conekta_payments`
--
ALTER TABLE `conekta_payments`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=18;

--
-- AUTO_INCREMENT de la tabla `cortes_almacen`
--
ALTER TABLE `cortes_almacen`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT de la tabla `cortes_almacen_detalle`
--
ALTER TABLE `cortes_almacen_detalle`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=186;

--
-- AUTO_INCREMENT de la tabla `corte_caja`
--
ALTER TABLE `corte_caja`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=83;

--
-- AUTO_INCREMENT de la tabla `corte_caja_historial`
--
ALTER TABLE `corte_caja_historial`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=19;

--
-- AUTO_INCREMENT de la tabla `desglose_corte`
--
ALTER TABLE `desglose_corte`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=340;

--
-- AUTO_INCREMENT de la tabla `entradas_detalle`
--
ALTER TABLE `entradas_detalle`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `entradas_insumo`
--
ALTER TABLE `entradas_insumo`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `facturas`
--
ALTER TABLE `facturas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT de la tabla `factura_detalles`
--
ALTER TABLE `factura_detalles`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=8;

--
-- AUTO_INCREMENT de la tabla `horarios`
--
ALTER TABLE `horarios`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=8;

--
-- AUTO_INCREMENT de la tabla `insumos`
--
ALTER TABLE `insumos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=192;

--
-- AUTO_INCREMENT de la tabla `logs_accion`
--
ALTER TABLE `logs_accion`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=980;

--
-- AUTO_INCREMENT de la tabla `log_asignaciones_mesas`
--
ALTER TABLE `log_asignaciones_mesas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=16;

--
-- AUTO_INCREMENT de la tabla `log_cancelaciones`
--
ALTER TABLE `log_cancelaciones`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=20;

--
-- AUTO_INCREMENT de la tabla `log_mesas`
--
ALTER TABLE `log_mesas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=34;

--
-- AUTO_INCREMENT de la tabla `menu_dia`
--
ALTER TABLE `menu_dia`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT de la tabla `mesas`
--
ALTER TABLE `mesas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=21;

--
-- AUTO_INCREMENT de la tabla `movimientos_caja`
--
ALTER TABLE `movimientos_caja`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=9;

--
-- AUTO_INCREMENT de la tabla `movimientos_insumos`
--
ALTER TABLE `movimientos_insumos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=106;

--
-- AUTO_INCREMENT de la tabla `ofertas_dia`
--
ALTER TABLE `ofertas_dia`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT de la tabla `productos`
--
ALTER TABLE `productos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=9006;

--
-- AUTO_INCREMENT de la tabla `proveedores`
--
ALTER TABLE `proveedores`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=15;

--
-- AUTO_INCREMENT de la tabla `qrs_insumo`
--
ALTER TABLE `qrs_insumo`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=26;

--
-- AUTO_INCREMENT de la tabla `recetas`
--
ALTER TABLE `recetas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1281;

--
-- AUTO_INCREMENT de la tabla `repartidores`
--
ALTER TABLE `repartidores`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;

--
-- AUTO_INCREMENT de la tabla `rutas`
--
ALTER TABLE `rutas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=33;

--
-- AUTO_INCREMENT de la tabla `sedes`
--
ALTER TABLE `sedes`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT de la tabla `tickets`
--
ALTER TABLE `tickets`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=199;

--
-- AUTO_INCREMENT de la tabla `ticket_descuentos`
--
ALTER TABLE `ticket_descuentos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=36;

--
-- AUTO_INCREMENT de la tabla `ticket_detalles`
--
ALTER TABLE `ticket_detalles`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=328;

--
-- AUTO_INCREMENT de la tabla `usuarios`
--
ALTER TABLE `usuarios`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=38;

--
-- AUTO_INCREMENT de la tabla `usuario_ruta`
--
ALTER TABLE `usuario_ruta`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=130;

--
-- AUTO_INCREMENT de la tabla `ventas`
--
ALTER TABLE `ventas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=346;

--
-- AUTO_INCREMENT de la tabla `venta_detalles`
--
ALTER TABLE `venta_detalles`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1373;

--
-- AUTO_INCREMENT de la tabla `venta_detalles_cancelados`
--
ALTER TABLE `venta_detalles_cancelados`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=11;

--
-- AUTO_INCREMENT de la tabla `venta_detalles_log`
--
ALTER TABLE `venta_detalles_log`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=586;

--
-- Restricciones para tablas volcadas
--

--
-- Filtros para la tabla `cortes_almacen`
--
ALTER TABLE `cortes_almacen`
  ADD CONSTRAINT `cortes_almacen_ibfk_1` FOREIGN KEY (`usuario_abre_id`) REFERENCES `usuarios` (`id`),
  ADD CONSTRAINT `cortes_almacen_ibfk_2` FOREIGN KEY (`usuario_cierra_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `cortes_almacen_detalle`
--
ALTER TABLE `cortes_almacen_detalle`
  ADD CONSTRAINT `cortes_almacen_detalle_ibfk_1` FOREIGN KEY (`corte_id`) REFERENCES `cortes_almacen` (`id`),
  ADD CONSTRAINT `cortes_almacen_detalle_ibfk_2` FOREIGN KEY (`insumo_id`) REFERENCES `insumos` (`id`);

--
-- Filtros para la tabla `corte_caja`
--
ALTER TABLE `corte_caja`
  ADD CONSTRAINT `corte_caja_ibfk_1` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `corte_caja_historial`
--
ALTER TABLE `corte_caja_historial`
  ADD CONSTRAINT `corte_caja_historial_ibfk_1` FOREIGN KEY (`corte_id`) REFERENCES `corte_caja` (`id`),
  ADD CONSTRAINT `corte_caja_historial_ibfk_2` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `desglose_corte`
--
ALTER TABLE `desglose_corte`
  ADD CONSTRAINT `desglose_corte_ibfk_1` FOREIGN KEY (`corte_id`) REFERENCES `corte_caja` (`id`),
  ADD CONSTRAINT `fk_denominacion` FOREIGN KEY (`denominacion_id`) REFERENCES `catalogo_denominaciones` (`id`);

--
-- Filtros para la tabla `entradas_detalle`
--
ALTER TABLE `entradas_detalle`
  ADD CONSTRAINT `entradas_detalle_ibfk_1` FOREIGN KEY (`entrada_id`) REFERENCES `entradas_insumo` (`id`),
  ADD CONSTRAINT `fk_entrada_detalle_insumo` FOREIGN KEY (`insumo_id`) REFERENCES `insumos` (`id`);

--
-- Filtros para la tabla `entradas_insumo`
--
ALTER TABLE `entradas_insumo`
  ADD CONSTRAINT `entradas_insumo_ibfk_1` FOREIGN KEY (`proveedor_id`) REFERENCES `proveedores` (`id`),
  ADD CONSTRAINT `fk_entrada_usuario` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `facturas`
--
ALTER TABLE `facturas`
  ADD CONSTRAINT `fk_fact_cliente` FOREIGN KEY (`cliente_id`) REFERENCES `clientes_facturacion` (`id`),
  ADD CONSTRAINT `fk_fact_ticket` FOREIGN KEY (`ticket_id`) REFERENCES `tickets` (`id`);

--
-- Filtros para la tabla `factura_detalles`
--
ALTER TABLE `factura_detalles`
  ADD CONSTRAINT `fk_fd_fact` FOREIGN KEY (`factura_id`) REFERENCES `facturas` (`id`);

--
-- Filtros para la tabla `factura_tickets`
--
ALTER TABLE `factura_tickets`
  ADD CONSTRAINT `fk_ft_factura` FOREIGN KEY (`factura_id`) REFERENCES `facturas` (`id`) ON UPDATE CASCADE,
  ADD CONSTRAINT `fk_ft_ticket` FOREIGN KEY (`ticket_id`) REFERENCES `tickets` (`id`) ON UPDATE CASCADE;

--
-- Filtros para la tabla `fondo`
--
ALTER TABLE `fondo`
  ADD CONSTRAINT `fondo_ibfk_1` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `horarios`
--
ALTER TABLE `horarios`
  ADD CONSTRAINT `fk_horario_serie` FOREIGN KEY (`serie_id`) REFERENCES `catalogo_folios` (`id`);

--
-- Filtros para la tabla `log_asignaciones_mesas`
--
ALTER TABLE `log_asignaciones_mesas`
  ADD CONSTRAINT `log_asignaciones_mesas_ibfk_1` FOREIGN KEY (`mesa_id`) REFERENCES `mesas` (`id`),
  ADD CONSTRAINT `log_asignaciones_mesas_ibfk_2` FOREIGN KEY (`mesero_anterior_id`) REFERENCES `usuarios` (`id`),
  ADD CONSTRAINT `log_asignaciones_mesas_ibfk_3` FOREIGN KEY (`mesero_nuevo_id`) REFERENCES `usuarios` (`id`),
  ADD CONSTRAINT `log_asignaciones_mesas_ibfk_4` FOREIGN KEY (`usuario_que_asigna_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `log_mesas`
--
ALTER TABLE `log_mesas`
  ADD CONSTRAINT `log_mesas_ibfk_1` FOREIGN KEY (`mesa_id`) REFERENCES `mesas` (`id`),
  ADD CONSTRAINT `log_mesas_ibfk_2` FOREIGN KEY (`venta_id`) REFERENCES `ventas` (`id`),
  ADD CONSTRAINT `log_mesas_ibfk_3` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `mesas`
--
ALTER TABLE `mesas`
  ADD CONSTRAINT `fk_mesa_area` FOREIGN KEY (`area_id`) REFERENCES `catalogo_areas` (`id`),
  ADD CONSTRAINT `fk_mesas_alineacion` FOREIGN KEY (`alineacion_id`) REFERENCES `alineacion` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
  ADD CONSTRAINT `mesas_ibfk_1` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `movimientos_caja`
--
ALTER TABLE `movimientos_caja`
  ADD CONSTRAINT `movimientos_caja_ibfk_1` FOREIGN KEY (`corte_id`) REFERENCES `corte_caja` (`id`),
  ADD CONSTRAINT `movimientos_caja_ibfk_2` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `productos`
--
ALTER TABLE `productos`
  ADD CONSTRAINT `fk_productos_categoria` FOREIGN KEY (`categoria_id`) REFERENCES `catalogo_categorias` (`id`);

--
-- Filtros para la tabla `recetas`
--
ALTER TABLE `recetas`
  ADD CONSTRAINT `recetas_ibfk_1` FOREIGN KEY (`producto_id`) REFERENCES `productos` (`id`),
  ADD CONSTRAINT `recetas_ibfk_2` FOREIGN KEY (`insumo_id`) REFERENCES `insumos` (`id`);

--
-- Filtros para la tabla `tickets`
--
ALTER TABLE `tickets`
  ADD CONSTRAINT `fk_ticket_cheque_banco` FOREIGN KEY (`cheque_banco_id`) REFERENCES `catalogo_bancos` (`id`),
  ADD CONSTRAINT `fk_ticket_tarjeta_banco` FOREIGN KEY (`tarjeta_banco_id`) REFERENCES `catalogo_bancos` (`id`),
  ADD CONSTRAINT `fk_ticket_tarjeta_marca` FOREIGN KEY (`tarjeta_marca_id`) REFERENCES `catalogo_tarjetas` (`id`),
  ADD CONSTRAINT `fk_tickets_sede` FOREIGN KEY (`sede_id`) REFERENCES `sedes` (`id`),
  ADD CONSTRAINT `fk_tickets_serie` FOREIGN KEY (`serie_id`) REFERENCES `catalogo_folios` (`id`),
  ADD CONSTRAINT `tickets_ibfk_1` FOREIGN KEY (`venta_id`) REFERENCES `ventas` (`id`),
  ADD CONSTRAINT `tickets_ibfk_2` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `ticket_descuentos`
--
ALTER TABLE `ticket_descuentos`
  ADD CONSTRAINT `fk_td_ticket` FOREIGN KEY (`ticket_id`) REFERENCES `tickets` (`id`),
  ADD CONSTRAINT `fk_td_vd` FOREIGN KEY (`venta_detalle_id`) REFERENCES `venta_detalles` (`id`);

--
-- Filtros para la tabla `ticket_detalles`
--
ALTER TABLE `ticket_detalles`
  ADD CONSTRAINT `ticket_detalles_ibfk_1` FOREIGN KEY (`ticket_id`) REFERENCES `tickets` (`id`),
  ADD CONSTRAINT `ticket_detalles_ibfk_2` FOREIGN KEY (`producto_id`) REFERENCES `productos` (`id`);

--
-- Filtros para la tabla `usuario_ruta`
--
ALTER TABLE `usuario_ruta`
  ADD CONSTRAINT `usuario_ruta_ibfk_1` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`) ON DELETE CASCADE,
  ADD CONSTRAINT `usuario_ruta_ibfk_2` FOREIGN KEY (`ruta_id`) REFERENCES `rutas` (`id`) ON DELETE CASCADE;

--
-- Filtros para la tabla `ventas`
--
ALTER TABLE `ventas`
  ADD CONSTRAINT `fk_corte` FOREIGN KEY (`corte_id`) REFERENCES `corte_caja` (`id`),
  ADD CONSTRAINT `fk_venta_corte` FOREIGN KEY (`corte_id`) REFERENCES `corte_caja` (`id`),
  ADD CONSTRAINT `fk_ventas_sede` FOREIGN KEY (`sede_id`) REFERENCES `sedes` (`id`),
  ADD CONSTRAINT `ventas_ibfk_1` FOREIGN KEY (`mesa_id`) REFERENCES `mesas` (`id`),
  ADD CONSTRAINT `ventas_ibfk_2` FOREIGN KEY (`repartidor_id`) REFERENCES `repartidores` (`id`),
  ADD CONSTRAINT `ventas_ibfk_3` FOREIGN KEY (`usuario_id`) REFERENCES `usuarios` (`id`),
  ADD CONSTRAINT `ventas_ibfk_4` FOREIGN KEY (`cajero_id`) REFERENCES `usuarios` (`id`);

--
-- Filtros para la tabla `venta_detalles`
--
ALTER TABLE `venta_detalles`
  ADD CONSTRAINT `venta_detalles_ibfk_1` FOREIGN KEY (`venta_id`) REFERENCES `ventas` (`id`),
  ADD CONSTRAINT `venta_detalles_ibfk_2` FOREIGN KEY (`producto_id`) REFERENCES `productos` (`id`);

--
-- Filtros para la tabla `venta_detalles_log`
--
ALTER TABLE `venta_detalles_log`
  ADD CONSTRAINT `venta_detalles_log_ibfk_1` FOREIGN KEY (`venta_detalle_id`) REFERENCES `venta_detalles` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

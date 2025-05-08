-- Parte 2: Desarrollo
-- Base de Datos: Préstamos de Libros

--------------------------------------------------------------------------
-- 1. TRIGGER
--------------------------------------------------------------------------
-- Trigger para actualizar el estado de la copia en la tabla COPIAS
-- cuando se inserta un nuevo préstamo o se registra una devolución.
--------------------------------------------------------------------------
CREATE TRIGGER TRG_ActualizarEstadoCopia
ON PRESTAMO
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Caso 1: Se inserta un nuevo préstamo (o se actualiza a uno sin fecha de devolución)
    IF EXISTS (SELECT 1 FROM inserted WHERE F_DEVOL IS NULL)
    BEGIN
        UPDATE C
        SET ESTADO = 'P' -- P: Prestado
        FROM COPIAS C
        INNER JOIN inserted i ON C.NRO_LIBRO = i.NRO_LIBRO AND C.NRO_COPIA = i.NRO_COPIA
        WHERE i.F_DEVOL IS NULL;
    END

    -- Caso 2: Se registra una devolución (F_DEVOL tiene una fecha)
    IF EXISTS (SELECT 1 FROM inserted WHERE F_DEVOL IS NOT NULL)
    BEGIN
        -- Si una copia fue devuelta, se marca como Disponible 'D'
        -- Esto asume que una vez devuelta, la copia está inmediatamente disponible.
        -- Podría haber lógica más compleja aquí si una copia devuelta necesita revisión.
        UPDATE C
        SET ESTADO = 'D' -- D: Disponible
        FROM COPIAS C
        INNER JOIN inserted i ON C.NRO_LIBRO = i.NRO_LIBRO AND C.NRO_COPIA = i.NRO_COPIA
        WHERE i.F_DEVOL IS NOT NULL;
    END
END;
GO

PRINT 'Trigger TRG_ActualizarEstadoCopia creado.'
GO

--------------------------------------------------------------------------
-- 2. STORED PROCEDURE
--------------------------------------------------------------------------
-- Stored Procedure para registrar un nuevo préstamo, validando
-- el estado del lector, del libro y de la copia.
--------------------------------------------------------------------------
CREATE PROCEDURE SP_RegistrarPrestamo
    @NRO_LECTOR INT,
    @NRO_LIBRO INT,
    @NRO_COPIA SMALLINT,
    @F_PREST DATETIME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @EstadoLector CHAR(1);
    DECLARE @EstadoCopia CHAR(1);
    DECLARE @EstadoLibro CHAR(1);
    DECLARE @MsgError VARCHAR(500);

    -- Validar Lector
    SELECT @EstadoLector = ESTADO FROM LECTOR WHERE NRO_LECTOR = @NRO_LECTOR;
    IF @EstadoLector IS NULL
    BEGIN
        SET @MsgError = 'Error: Lector con NRO_LECTOR ' + CAST(@NRO_LECTOR AS VARCHAR) + ' no existe.';
        RAISERROR(@MsgError, 16, 1);
        RETURN;
    END
    IF @EstadoLector = 'I' -- Inhabilitado
    BEGIN
        SET @MsgError = 'Error: Lector con NRO_LECTOR ' + CAST(@NRO_LECTOR AS VARCHAR) + ' está inhabilitado (Estado: I).';
        RAISERROR(@MsgError, 16, 1);
        RETURN;
    END

    -- Validar Libro
    SELECT @EstadoLibro = ESTADO FROM LIBRO WHERE NRO_LIBRO = @NRO_LIBRO;
    IF @EstadoLibro IS NULL
    BEGIN
        SET @MsgError = 'Error: Libro con NRO_LIBRO ' + CAST(@NRO_LIBRO AS VARCHAR) + ' no existe.';
        RAISERROR(@MsgError, 16, 1);
        RETURN;
    END
    IF @EstadoLibro = 'N' -- No Disponible
    BEGIN
        SET @MsgError = 'Error: Libro con NRO_LIBRO ' + CAST(@NRO_LIBRO AS VARCHAR) + ' no está disponible (Estado: N).';
        RAISERROR(@MsgError, 16, 1);
        RETURN;
    END

    -- Validar Copia
    SELECT @EstadoCopia = ESTADO FROM COPIAS WHERE NRO_LIBRO = @NRO_LIBRO AND NRO_COPIA = @NRO_COPIA;
    IF @EstadoCopia IS NULL
    BEGIN
        SET @MsgError = 'Error: Copia con NRO_LIBRO ' + CAST(@NRO_LIBRO AS VARCHAR) + ' y NRO_COPIA ' + CAST(@NRO_COPIA AS VARCHAR) + ' no existe.';
        RAISERROR(@MsgError, 16, 1);
        RETURN;
    END
    IF @EstadoCopia != 'D' -- Debe estar Disponible para prestar
    BEGIN
        SET @MsgError = 'Error: Copia NRO_COPIA ' + CAST(@NRO_COPIA AS VARCHAR) + ' del LIBRO ' + CAST(@NRO_LIBRO AS VARCHAR) + ' no está disponible para préstamo (Estado: ' + @EstadoCopia + ').';
        RAISERROR(@MsgError, 16, 1);
        RETURN;
    END

    -- Si todas las validaciones son correctas, insertar el préstamo
    BEGIN TRY
        INSERT INTO PRESTAMO (NRO_LECTOR, NRO_LIBRO, NRO_COPIA, F_PREST, F_DEVOL)
        VALUES (@NRO_LECTOR, @NRO_LIBRO, @NRO_COPIA, @F_PREST, NULL);

        PRINT 'Préstamo registrado exitosamente para Lector: ' + CAST(@NRO_LECTOR AS VARCHAR) + ', Libro: ' + CAST(@NRO_LIBRO AS VARCHAR) + ', Copia: ' + CAST(@NRO_COPIA AS VARCHAR) + '.';
    END TRY
    BEGIN CATCH
        -- Re-lanzar el error si la inserción falla por otra razón (ej. PK duplicada si se intenta el mismo día)
        THROW;
    END CATCH
END;
GO

PRINT 'Stored Procedure SP_RegistrarPrestamo creado.'
GO

--------------------------------------------------------------------------
-- 3. FUNCIÓN
--------------------------------------------------------------------------
-- Función para obtener la descripción de un tipo de libro.
--------------------------------------------------------------------------
CREATE FUNCTION FN_ObtenerDescripcionTipoLibro (@TIPO CHAR(2))
RETURNS CHAR(40)
AS
BEGIN
    DECLARE @DESCTIPO CHAR(40);

    SELECT @DESCTIPO = DESCTIPO
    FROM TIPOLIBRO
    WHERE TIPO = @TIPO;

    RETURN @DESCTIPO;
END;
GO

PRINT 'Función FN_ObtenerDescripcionTipoLibro creada.'
GO

--------------------------------------------------------------------------
-- 4. TRANSACCIÓN (Script con TRY-CATCH)
--------------------------------------------------------------------------
-- Script que simula el registro de un préstamo y su devolución
-- dentro de una transacción, con manejo de Commit y Rollback.
--------------------------------------------------------------------------
PRINT 'Ejecutando script de ejemplo de Transacción...'
BEGIN
    -- Suponemos que queremos registrar un préstamo y luego inmediatamente su devolución
    -- (esto es solo para ejemplificar la transacción, la lógica de negocio podría ser diferente)
    DECLARE @NRO_LECTOR_TR INT = 123456; -- Lector de ejemplo
    DECLARE @NRO_LIBRO_TR INT = 10545377; -- Libro de ejemplo
    DECLARE @NRO_COPIA_TR SMALLINT = 1; -- Copia de ejemplo
    DECLARE @F_PREST_TR DATETIME = GETDATE();
    DECLARE @F_DEVOL_TR DATETIME = DATEADD(day, 7, GETDATE()); -- Devolución en 7 días
    DECLARE @ErrorOccurred BIT = 0;

    BEGIN TRANSACTION RegistroPrestamoDevolucion;

    BEGIN TRY
        PRINT 'Intentando registrar préstamo (dentro de la transacción)...';
        -- Se podría llamar a SP_RegistrarPrestamo aquí si se quiere,
        -- pero para este ejemplo, haremos la inserción directa para mayor control en el TRY.
        
        -- Validaciones (simplificadas para el ejemplo de transacción, el SP_RegistrarPrestamo es más completo)
        IF NOT EXISTS (SELECT 1 FROM LECTOR WHERE NRO_LECTOR = @NRO_LECTOR_TR AND ESTADO = 'H')
        BEGIN
            RAISERROR('TRANSACCION: Lector no habilitado o no existe.', 16, 1);
        END
        IF NOT EXISTS (SELECT 1 FROM COPIAS WHERE NRO_LIBRO = @NRO_LIBRO_TR AND NRO_COPIA = @NRO_COPIA_TR AND ESTADO = 'D')
        BEGIN
            RAISERROR('TRANSACCION: Copia no disponible para préstamo.', 16, 1);
        END

        INSERT INTO PRESTAMO (NRO_LECTOR, NRO_LIBRO, NRO_COPIA, F_PREST, F_DEVOL)
        VALUES (@NRO_LECTOR_TR, @NRO_LIBRO_TR, @NRO_COPIA_TR, @F_PREST_TR, NULL); -- Primero se presta

        PRINT 'Préstamo registrado en la transacción. NRO_LECTOR: ' + CAST(@NRO_LECTOR_TR AS VARCHAR) + ', LIBRO: ' + CAST(@NRO_LIBRO_TR AS VARCHAR) + ', COPIA: ' + CAST(@NRO_COPIA_TR AS VARCHAR);

        -- Simular una actualización para registrar la devolución
        PRINT 'Intentando registrar devolución (dentro de la transacción)...';
        UPDATE PRESTAMO
        SET F_DEVOL = @F_DEVOL_TR
        WHERE NRO_LECTOR = @NRO_LECTOR_TR
          AND NRO_LIBRO = @NRO_LIBRO_TR
          AND NRO_COPIA = @NRO_COPIA_TR
          AND F_PREST = @F_PREST_TR; -- Asegurar que actualizamos el préstamo correcto

        IF @@ROWCOUNT = 0
        BEGIN
            RAISERROR('TRANSACCION: No se encontró el préstamo original para actualizar la devolución.', 16, 1);
        END
        
        PRINT 'Devolución registrada en la transacción.';

        -- Si todo fue bien, confirmar la transacción
        IF XACT_STATE() = 1 -- Transacción activa y confirmable
        BEGIN
            COMMIT TRANSACTION RegistroPrestamoDevolucion;
            PRINT 'Transacción confirmada (COMMIT).';
        END
    END TRY
    BEGIN CATCH
        SET @ErrorOccurred = 1;
        PRINT 'Error detectado en la transacción: ' + ERROR_MESSAGE();
        IF XACT_STATE() <> 0 -- Si la transacción está activa (puede ser -1 si no es confirmable)
        BEGIN
            ROLLBACK TRANSACTION RegistroPrestamoDevolucion;
            PRINT 'Transacción revertida (ROLLBACK).';
        END
    END CATCH

    IF @ErrorOccurred = 0 AND XACT_STATE() = 0
        PRINT 'Proceso de transacción completado sin errores explícitos y transacción cerrada.';
    ELSE IF @ErrorOccurred = 1
        PRINT 'Proceso de transacción finalizado con errores.';
END
GO

PRINT 'Script de Transacción (ejemplo) definido.'
GO

--------------------------------------------------------------------------
-- 5. VISTA CON INCONSISTENCIAS
--------------------------------------------------------------------------
-- Vista para detectar inconsistencias entre el estado de las copias
-- y los registros de préstamos.
--------------------------------------------------------------------------
CREATE VIEW VW_InconsistenciasPrestamosCopias
AS
SELECT
    C.NRO_LIBRO,
    C.NRO_COPIA,
    C.ESTADO AS EstadoCopiaTabla,
    P_ACTIVO.F_PREST AS FechaPrestamoActivo,
    P_ACTIVO.F_DEVOL AS FechaDevolucionPrestamoActivo,
    CASE
        WHEN C.ESTADO = 'D' AND P_ACTIVO.NRO_LECTOR IS NOT NULL THEN 'INCONSISTENCIA: Copia DISPONIBLE (D) pero tiene un préstamo activo (F_DEVOL es NULL).'
        WHEN C.ESTADO = 'P' AND P_ACTIVO.NRO_LECTOR IS NULL THEN 'INCONSISTENCIA: Copia PRESTADA (P) pero NO tiene un préstamo activo correspondiente (F_DEVOL es NULL).'
        WHEN C.ESTADO = 'N' AND P_ACTIVO.NRO_LECTOR IS NOT NULL THEN 'INCONSISTENCIA: Copia NO DISPONIBLE (N) pero tiene un préstamo activo (F_DEVOL es NULL).'
      ELSE 'Sin inconsistencia aparente por esta lógica.'
    END AS MotivoInconsistencia
FROM
    COPIAS C
LEFT JOIN (
    -- Subconsulta para obtener solo el préstamo activo más reciente (si hay varios para la misma copia, lo cual no debería suceder si la PK de PRESTAMO es correcta)
    -- o simplemente el préstamo activo. La PK de PRESTAMO incluye F_PREST, por lo que no debería haber múltiples préstamos *activos* para la misma copia al mismo tiempo.
    SELECT *
    FROM PRESTAMO P
    WHERE P.F_DEVOL IS NULL
) AS P_ACTIVO ON C.NRO_LIBRO = P_ACTIVO.NRO_LIBRO AND C.NRO_COPIA = P_ACTIVO.NRO_COPIA
WHERE
    (C.ESTADO = 'D' AND P_ACTIVO.NRO_LECTOR IS NOT NULL) OR
    (C.ESTADO = 'P' AND P_ACTIVO.NRO_LECTOR IS NULL AND NOT EXISTS (SELECT 1 FROM PRESTAMO P_ANY WHERE P_ANY.NRO_LIBRO = C.NRO_LIBRO AND P_ANY.NRO_COPIA = C.NRO_COPIA AND P_ANY.F_DEVOL IS NULL)) OR
    (C.ESTADO = 'N' AND P_ACTIVO.NRO_LECTOR IS NOT NULL);

GO

PRINT 'Vista VW_InconsistenciasPrestamosCopias creada.'
GO

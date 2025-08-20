BEGIN
  FOR i IN 0..49 LOOP  -- 50 lần x 100.000 = 5 triệu
    INSERT INTO notes (note)
    SELECT 'Ghi chú số ' || (i * 100000 + level)
    FROM dual
    CONNECT BY level <= 100000;

    COMMIT;
  END LOOP;
END;
/

select * from notes;
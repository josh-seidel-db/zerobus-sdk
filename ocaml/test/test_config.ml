(* Unit tests for Zerobus_core.Config URL parsing — locks in the workspace_id
   derivation fix (a live test caught that an Azure "adb-<id>" URL yielded the
   whole label "adb-984752964297111" instead of the numeric id "984752964297111",
   poisoning the OAuth token audience). The id is the first maximal DIGIT RUN in
   the first host label. *)

module Config = Zerobus_core.Config

let wsid_ok label url expected =
  Alcotest.test_case label `Quick (fun () ->
      match Config.workspace_id_of_url url with
      | Ok got -> Alcotest.(check string) url expected got
      | Error e ->
          Alcotest.failf "%s: unexpected Error %s" url
            (Zerobus_core.Error.to_string e))

let wsid_err label url =
  Alcotest.test_case label `Quick (fun () ->
      match Config.workspace_id_of_url url with
      | Error _ -> ()
      | Ok got -> Alcotest.failf "%s: expected Error, got Ok %S" url got)

let () =
  Alcotest.run "config"
    [
      ( "workspace_id_of_url",
        [
          (* Azure adb- form: the regression the live test caught. The numeric id
             follows the "adb-" scheme prefix; must NOT return the whole label. *)
          wsid_ok "azure-adb-https"
            "https://adb-984752964297111.11.azuredatabricks.net"
            "984752964297111";
          wsid_ok "azure-adb-no-scheme"
            "adb-984752964297111.11.azuredatabricks.net" "984752964297111";
          (* GCP: purely numeric first label. *)
          wsid_ok "gcp-numeric"
            "https://1234567890123456.7.gcp.databricks.com" "1234567890123456";
          (* A bare numeric id label. *)
          wsid_ok "bare-numeric" "https://5551234.cloud.databricks.com" "5551234";
          (* Trailing http:// scheme is also stripped. *)
          wsid_ok "http-scheme" "http://adb-42.9.azuredatabricks.net" "42";
          (* First digit run wins over any later digits in the same label. *)
          wsid_ok "first-run-wins" "adb-42-99.azuredatabricks.net" "42";
          (* No digits anywhere in the first label -> Error, not a bad guess. *)
          wsid_err "no-digits" "https://myhost.example.com";
          wsid_err "empty" "";
        ] );
    ]

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

(* endpoint_of_workspace: expect a specific derived (host, port). *)
let ep_ok label ~endpoint ~workspace_url exp_host exp_port =
  Alcotest.test_case label `Quick (fun () ->
      match Config.endpoint_of_workspace ~endpoint ~workspace_url with
      | Ok (host, port) ->
          Alcotest.(check string) (label ^ " host") exp_host host;
          Alcotest.(check int) (label ^ " port") exp_port port
      | Error e ->
          Alcotest.failf "%s: unexpected Error %s" label
            (Zerobus_core.Error.to_string e))

(* endpoint_of_workspace: expect an Error (we refuse to guess a wrong host). *)
let ep_err label ~endpoint ~workspace_url =
  Alcotest.test_case label `Quick (fun () ->
      match Config.endpoint_of_workspace ~endpoint ~workspace_url with
      | Error _ -> ()
      | Ok (host, _) ->
          Alcotest.failf "%s: expected Error, got Ok host=%S" label host)

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
      ( "endpoint_of_workspace",
        [
          (* Explicit endpoint is used verbatim (host or host:port). *)
          ep_ok "explicit-host-port" ~endpoint:"1.2.3.4:9443"
            ~workspace_url:"" "1.2.3.4" 9443;
          ep_ok "explicit-host-only" ~endpoint:"myhost.example.com"
            ~workspace_url:"" "myhost.example.com" 443;
          (* Already-zerobus-form workspace URLs (region present) → used directly,
             per cloud. *)
          ep_ok "aws-zerobus-form" ~endpoint:""
            ~workspace_url:"https://1234567890.zerobus.us-west-2.cloud.databricks.com"
            "1234567890.zerobus.us-west-2.cloud.databricks.com" 443;
          ep_ok "azure-zerobus-form" ~endpoint:""
            ~workspace_url:"https://984752964297111.zerobus.eastus2.azuredatabricks.net"
            "984752964297111.zerobus.eastus2.azuredatabricks.net" 443;
          ep_ok "gcp-zerobus-form" ~endpoint:""
            ~workspace_url:"https://1234567890123456.zerobus.us-central1.gcp.databricks.com"
            "1234567890123456.zerobus.us-central1.gcp.databricks.com" 443;
          (* Console workspace URLs carry NO region → we must NOT guess a host; we
             return an actionable Error (per cloud) instead of the old wrong host. *)
          ep_err "aws-console-no-region" ~endpoint:""
            ~workspace_url:"https://dbc-a1b2c3d4-e5f6.cloud.databricks.com";
          ep_err "azure-console-no-region" ~endpoint:""
            ~workspace_url:"https://adb-984752964297111.11.azuredatabricks.net";
          ep_err "gcp-console-no-region" ~endpoint:""
            ~workspace_url:"https://1234567890123456.7.gcp.databricks.com";
          (* Unknown cloud → Error, never a bogus host. *)
          ep_err "unknown-cloud" ~endpoint:""
            ~workspace_url:"https://12345.example.com";
        ] );
    ]

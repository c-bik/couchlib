-module(couchlib).

-include_lib("couch/couch_db.hrl").

-export([start/0]).

-export([open/1, open/2,
         close/1,
         delete_db/1, delete_db/2,
         info/1, info/2,
         all_dbs/0,
         put/2, put/3,
         put_many/2,
         get/2, get/3,
         delete/2,
         select/2, select/3, select/4,
         first/1, next/2, last/1, prev/2,
         start_compact/1]).

-define(SUP, couchlib_sup).

%% ---------------------------------------------------------------------------
%% @doc Convenience function to start couchlib app and dependent applications.
%%
%% In production deployments, use Erlang releases, which are better suited at
%% handling startup dependencies.
%% ---------------------------------------------------------------------------
start() ->
    application:start(sasl),
    application:start(crypto),
    application:start(couchlib).

%% ---------------------------------------------------------------------------
%% @doc Open a database. If the database doesn't exist, it's created.
%%
%% On success, returns {ok, Db} otherwise returns an applicable error.
%%
%% TODO: document options
%% TODO: confirm that options make sense for create as well as open, else
%%       we need two functions
%% ---------------------------------------------------------------------------

open(Name) ->
    open(Name, []).

open(Name, Options0) ->
    Options = maybe_add_admin_role(Options0),
    case couch_db:open(db_name(Name), Options) of
        {not_found, no_db_file} ->
            couch_server:create(db_name(Name), Options);
        Other -> Other
    end.

%% ---------------------------------------------------------------------------
%% @doc Closes a database.
%% ---------------------------------------------------------------------------

close(Db) ->
    couch_db:close(Db).

%% ---------------------------------------------------------------------------
%% @doc Deletes a database.
%%
%% Returns ok if deleted successfully otherwise returns an applicable error.
%%
%% TODO: document options
%% ---------------------------------------------------------------------------

delete_db(Name) ->
    delete_db(Name, []).

delete_db(Name, Options) ->
    couch_server:delete(db_name(Name), Options).

%% ---------------------------------------------------------------------------
%% @doc Returns info about the specified database.
%%
%% Error if Db is invalid.
%% ---------------------------------------------------------------------------

info(#db{}=Db0) ->
    Db = reopen(Db0),
    {ok, Info} = couch_db:get_db_info(Db),
    [info_item(I) || I <- Info];
info(Name) when is_list(Name) ->
    case couch_db:open(db_name(Name), []) of
        {not_found, no_db_file} -> undefined;
        {ok, Db} -> 
            try 
                info(Db) 
            after 
                couch_db:close(Db) 
            end
    end.

info(Db, InfoName) ->
    case info(Db) of
        undefined -> undefined;
        Info ->
            case proplists:get_value(InfoName, Info) of
                undefined ->
                    erlang:error(badarg);
                Val -> {InfoName, Val}
            end
    end.

info_item({db_name, B}) ->
    {db_name, binary_to_list(B)};
info_item({instance_start_time, B}) -> 
    {instance_start_time, list_to_integer(binary_to_list(B))};
info_item({_, _}=I) -> I.

%% ---------------------------------------------------------------------------
%% @doc Returns a list of all db names.
%% ---------------------------------------------------------------------------

all_dbs() ->
    {ok, Dbs} = couch_server:all_databases(),
    lists:map(fun binary_to_list/1, Dbs).

%% ---------------------------------------------------------------------------
%% @doc Stores a document in a db.
%%
%% Use couchdoc to create a document.
%%
%% Returns ???
%%
%% TODO: lots more here
%% TODO: missing UpdateType arg variant -- how used?
%% ---------------------------------------------------------------------------

put(Db, Doc) ->
    put(Db, Doc, []).

put(#db{}=Db0, #doc{}=Doc, Options) ->
    Db = reopen(Db0),
    case couch_db:update_doc(Db, Doc, Options) of
        {ok, {Start, RevId}} -> 
            {ok, Doc#doc{revs={Start, [RevId]}}};
        Err -> Err
    end;
put(_, _, _) -> erlang:error(badarg).

%% ---------------------------------------------------------------------------
%% @doc Stores documents in bulk.
%% ---------------------------------------------------------------------------

put_many(#db{}=Db0, Docs) ->
    Db = reopen(Db0),
    {ok, Result} = couch_db:update_docs(Db, Docs),
    lists:zipwith(fun({ok, {Start, RevId}}, Doc) -> 
                          {ok, Doc#doc{revs={Start, [RevId]}}};
                     (Err, Doc) ->
                          {Err, Doc}
                  end, Result, Docs).

%% ---------------------------------------------------------------------------
%% @doc Retrieves a document.
%%
%% TODO: more here
%% ---------------------------------------------------------------------------

get(Db, Id) when is_binary(Id) ->
    get(Db, Id, []).

get(#db{}=Db0, Id, Options) when is_binary(Id) ->
    Db = reopen(Db0),
    couch_db:open_doc(Db, Id, Options).

%% ---------------------------------------------------------------------------
%% @doc Removes Doc from Db.
%%
%% TODO: Letting delete_doc/2 "result" through -- what is this and how
%% would someone use it?
%% ---------------------------------------------------------------------------

delete(#db{}=Db0, #doc{}=Doc) ->
    Db = reopen(Db0),
    delete_doc(Db, Doc);
delete(#db{}=Db0, Id) when is_binary(Id) ->
    Db = reopen(Db0),
    case couch_db:open_doc(Db, Id, []) of
        {ok, Doc} ->
            delete_doc(Db, Doc);
        Err -> Err
    end.

delete_doc(Db, #doc{revs={Start, [Rev|_]}}=Doc) ->
    DelDoc = Doc#doc{revs={Start, [Rev]}, deleted=true},
    {ok, [Result]} = couch_db:update_docs(Db, [DelDoc], []),
    Result.

%% ---------------------------------------------------------------------------
%% @doc Selects all documents or rows. See select/4 for more information.
%% ---------------------------------------------------------------------------

select(Source, Options) ->
    select(Source, undefined, undefined, Options).

%% ---------------------------------------------------------------------------
%% @doc Selects all documents or rows that start with a specified ID or key.
%% Supported keys are binaries or lists of key parts (binary, atom, number).
%%
%% exlucde_end is ignored if specified in Options.
%% ---------------------------------------------------------------------------

select(Source, KeyOrId, Options) when is_binary(KeyOrId) orelse 
                                      is_list(KeyOrId) ->
    % Add 255 char to end of KeyOrId to extend the range to "starts with".
    End = case KeyOrId of
              V when is_binary(V) -> <<V/binary, 255>>;
              V when is_list(V) -> V ++ [255]
          end,
    select(Source, KeyOrId, End, proplists:delete(exclude_end, Options)).

%% ---------------------------------------------------------------------------
%% @doc Selects a range of documents using start and end keys.
%%
%%   select(Source, Start, End, Options) -> {TotalRowCount, Offset, Rows}
%%
%%   Source          = Db | {Db, Design, View}
%%   Db              = db()
%%   Design          = string()
%%   View            = string()
%%   Start           = key() | [key()]
%%   End             = key() | [key()]
%%   key()           = undefined | binary() | number() | atom()
%%   Options         = options()
%%   options()       = [option()]
%%   option()        = {limit, int()}
%%                   | {skip, int()}
%%                   | reverse
%%                   | exclude_end
%%                   | include_docs
%%   TotalRowCount   = int()
%%   Offset          = int()
%%   Rows            = [row()]
%%   row()           = [{id, binary()}, {key, binary()}, {value, term()},
%%                      {doc, proplist()}]
%%
%% TODO: how do you denote an optional property in a proplist (doc above)
%% ---------------------------------------------------------------------------

select(#db{}=Db0, StartId0, EndId0, Options)  ->
    Db = reopen(Db0),
    BaseArgs = view_query_base_args(Options),
    #view_query_args{direction=Dir,
                     limit=Limit,
                     skip=Skip} = BaseArgs,
    StartId = view_start(StartId0, Dir),
    EndId = view_end(EndId0, Dir),
    Args = BaseArgs#view_query_args{start_docid=StartId, end_docid=EndId},
    {ok, Info} = couch_db:get_db_info(Db),
    DocCount = couch_util:get_value(doc_count, Info),
    {ok, _, {_, _, _, Result}} = 
        couch_btree:fold(Db#db.fulldocinfo_by_id_btree,
                         enum_db_acc_fun(Db, DocCount, Args),
                         {Limit, Skip, undefined, []},
                         enum_db_options(Args)),
    case Result of
        {Offset, Rows} -> {DocCount, Offset, lists:reverse(Rows)};
        [] -> {DocCount, DocCount, []}
    end;

select({#db{}=Db0, Design, View}, StartKey, EndKey, Options) 
  when is_list(Design), is_list(View) ->
    Db = reopen(Db0),
    BaseArgs = view_query_base_args(Options),
    #view_query_args{stale=Stale,
                     limit=Limit,
                     skip=Skip} = BaseArgs,
    Args = BaseArgs#view_query_args{start_key=StartKey, end_key=EndKey},
    try couch_view:get_map_view(Db, design_id(Design), 
                                list_to_binary(View), Stale) of
        {ok, Map, _} ->
            {ok, RowCount} = couch_view:get_row_count(Map),
            case couch_view:fold(Map, fold_view_acc_fun(Db, RowCount, Args),
                                {Limit, Skip, undefined, []}, 
                                fold_view_options(Args)) of
                {ok, _, {_, _, _, {Offset, Rows}}} ->
                    {RowCount, Offset, lists:reverse(Rows)};
                {ok, _, {_, _, _, []}} ->
                    {RowCount, 0, []}
            end;
        {not_found, missing_named_view} -> {error, view_not_found}
    catch
        _:{not_found, missing} -> {error, design_not_found}
    end;

select({#db{}=Db0, Design, ReduceView, GroupLevel}, StartKey, EndKey, Options) 
  when is_list(Design), is_list(ReduceView) ->
    Db = reopen(Db0),
    BaseArgs = view_query_base_args(Options),
    #view_query_args{stale=Stale,
                     limit=Limit,
                     skip=Skip} = BaseArgs,
    Args = BaseArgs#view_query_args{start_key=StartKey, end_key=EndKey},
    try couch_view:get_reduce_view(Db, design_id(Design), 
                                list_to_binary(ReduceView), Stale) of
        {ok, Reduce, Group} ->
            {ok, GroupRowsFun, RespFun} = fold_reduce_view_acc_fun(Db, Group, GroupLevel),
            case couch_view:fold_reduce(Reduce, RespFun,
                    {Limit, Skip, undefined, []}, 
                    [{key_group_fun, GroupRowsFun}|fold_view_options(Args)]) of
                {ok, {_,_,_,Rows}} ->
                    {ok, Rows};
                Error ->
                    {error, Error}
            end;
        {not_found, missing_named_view} -> {error, view_not_found}
    catch
        _:{not_found, missing} -> {error, design_not_found}
    end.

%% ---------------------------------------------------------------------------
%% @doc Returns the first doc in the db or not_found if the db is empty.
%% ---------------------------------------------------------------------------

first(Db) ->
    case select(Db, [{limit, 1}]) of
        {_, _, []} -> not_found;
        {_, _, [[{id, Id}, _Key, _Val]]} -> Id
    end.

%% ---------------------------------------------------------------------------
%% @doc Returns the last doc ID in the db or not_found if the db is empty.
%% ---------------------------------------------------------------------------

last(Db) ->
    case select(Db, [{limit, 1}, reverse]) of
        {_, _, []} -> not_found;
        {_, _, [[{id, Id}, _Key, _Val]]} -> Id
    end.

%% ---------------------------------------------------------------------------
%% @doc Returns the next doc ID in the db given a doc ID or not_found if the
%% specified doc is the last doc.
%% ---------------------------------------------------------------------------

next(Db, Id) when is_binary(Id) ->
    case select(Db, Id, undefined, [{skip, 1 }, {limit, 1}]) of
        {_, _, []} -> not_found;
        {_, _, [[{id, NextId}, _Key, _Val]]} -> NextId
    end.

%% ---------------------------------------------------------------------------
%% @doc Returns the prev doc ID in the db given a doc ID or not_found if the
%% specified doc is the first doc.
%% ---------------------------------------------------------------------------

prev(Db, Id) when is_binary(Id) ->
    case select(Db, Id, undefined, [{skip, 1 }, {limit, 1}, reverse]) of
        {_, _, []} -> not_found;
        {_, _, [[{id, NextId}, _Key, _Val]]} -> NextId
    end.

start_compact(Db) ->
    couch_db:start_compact(Db).

%% ===========================================================================
%% Private functions
%% ===========================================================================

%% ---------------------------------------------------------------------------
%% @doc Reopens the database. This is needed for most db operations. We use the
%% #db record as our opaque db representation for convenience to the user.
%%
%% This has the upside of preserving user context when the db was open.
%% ---------------------------------------------------------------------------

reopen(#db{name=Name, user_ctx=Ctx}) ->
    {ok, Db} = open(Name, []),
    Db#db{user_ctx=Ctx}.

%% ---------------------------------------------------------------------------
%% @doc Ensures db names come back as binary (required by couch).
%% ---------------------------------------------------------------------------

db_name(Val) when is_binary(Val) -> Val;
db_name(Val) when is_list(Val) -> list_to_binary(Val).

%% ---------------------------------------------------------------------------
%% @doc Converts a proplist of options to a base view_query_args record. Used
%% by select functions.
%% ---------------------------------------------------------------------------

view_query_base_args(Options) ->
    Dir = case proplists:get_bool(reverse, Options) of
              true -> rev;
              false -> fwd
          end,
    Limit = proplists:get_value(limit, Options, 10000000000),
    Skip = proplists:get_value(skip, Options, 0),
    InclEnd = not proplists:get_bool(exclude_end, Options),
    InclDocs = proplists:get_bool(include_docs, Options),
    Stale = proplists:get_bool(stale, Options),
    #view_query_args{limit=Limit,
                     skip=Skip,
                     direction=Dir,
                     inclusive_end=InclEnd,
                     include_docs=InclDocs,
                     stale=Stale}.

%% ---------------------------------------------------------------------------
%% @doc Returns a design doc "id" for the give name.
%% ---------------------------------------------------------------------------

design_id(Name) when is_list(Name) ->
    NameBin = list_to_binary(Name),
    <<"_design/", NameBin/binary>>.

%% ---------------------------------------------------------------------------
%% @doc Returns a binary value for a given Start and Dir. If Start is
%% undefined, returns a value that is outside the range for Dir.
%% ---------------------------------------------------------------------------

view_start(undefined, Dir) -> unbound_start(Dir);
view_start(Id, _) when is_binary(Id) -> Id.

%% ---------------------------------------------------------------------------
%% @doc Returns a binary value that is outside the range for Dir.
%% ---------------------------------------------------------------------------

unbound_start(fwd) -> <<"">>;
unbound_start(rev) -> <<255>>. 

%% ---------------------------------------------------------------------------
%% @doc Same as view_start/2, but for the end value.
%% ---------------------------------------------------------------------------

view_end(undefined, Dir) -> unbound_end(Dir);
view_end(Id, _) when is_binary(Id) -> Id.

%% ---------------------------------------------------------------------------
%% @doc Same for unbound_start/1, but for the end value.
%% ---------------------------------------------------------------------------

unbound_end(fwd) -> <<255>>;    
unbound_end(rev) -> <<"">>. 

%% ---------------------------------------------------------------------------
%% @doc Returns a fun for use in couch_db:enum_docs. This can also be used
%% directly in a call to couch_btree:fold/4.
%%
%% This is a fairly complex function - the logic was largely taken from
%% hovercraft.
%% ---------------------------------------------------------------------------

enum_db_acc_fun(Db, DocCount, #view_query_args{}=Args) ->
    UpdateSeq = couch_db:get_update_seq(Db),
    Helpers = #view_fold_helper_funs{
      reduce_count=fun couch_db:enum_docs_reduce_to_count/1,
      start_response=fun doc_fold_start_response/6,
      send_row=fun doc_fold_send_row/5},
    FoldFun = couch_httpd_view:make_view_fold_fun(
                nil, Args, <<"">>, Db, UpdateSeq, DocCount, Helpers),
    fun(#full_doc_info{id=Id}=FDI, Offset, Acc) ->
            case couch_doc:to_doc_info(FDI) of
                #doc_info{revs=[#rev_info{deleted=false, rev=Rev}|_]} ->
                    FoldFun({{Id, Id}, {[{rev, couch_doc:rev_to_str(Rev)}]}},
                            Offset, Acc);
                #doc_info{revs=[#rev_info{deleted=true}|_]} ->
                    {ok, Acc}
            end
    end.

%% ---------------------------------------------------------------------------
%% @doc Returns a fun that can be used as the start_response field of a
%% view_fold_helper_funs record.
%%
%% Preserves the initial offset and provides an empty list for acc.
%% ---------------------------------------------------------------------------

doc_fold_reduce_start_response(_Req, _Etag, _Acc, _UpdateSeq) ->
    {ok, nil, []}.

doc_fold_start_response(_Req, _Etag, _RowCount, Offset, _Acc, _UpdateSeq) ->
    {ok, nil, {Offset, []}}.

%% ---------------------------------------------------------------------------
%% @doc Returns a fun that can be used as the send_row field of a
%% view_fold_helper_funs record.
%%
%% Preserves the offset provided and converts the doc into a view row object.
%%
%% Removes the outer tuple for "objects" to present them as tuple lists.
%% ---------------------------------------------------------------------------

doc_fold_reduce_send_row(_Resp, {Key, Value}, Acc) ->
    {ok, [{Key, Value}|Acc]}.

doc_fold_send_row(_Resp, Db, Doc, IncludeDocs, {Offset, Acc}) ->
    {Row} = couch_httpd_view:view_row_obj(Db, Doc, IncludeDocs),
    {ok, {Offset, [lists:map(fun format_row_item/1, Row)|Acc]}}.

%% ---------------------------------------------------------------------------
%% @doc Strips an outer tuple, e.g. {[]} -> [], which is used by couch to
%% represent objects. This is a more natural representation in Erlang, though
%% admittedly tuple lists can be confused with strings.
%% ---------------------------------------------------------------------------

format_row_item({Name, {Val}}) -> {Name, Val};
format_row_item({Name, Val}) -> {Name, Val}.

%% ---------------------------------------------------------------------------
%% @doc Returns a list of options that can be used when enumerating documents.
%% ---------------------------------------------------------------------------

enum_db_options(#view_query_args{start_docid=StartId,
                                 direction=Dir,
                                 inclusive_end=InclEnd,
                                 end_docid=EndId}) ->
    [{start_key, StartId}, {dir, Dir}, 
     {case InclEnd of true -> end_key; false -> end_key_gt end, EndId}].

%% ---------------------------------------------------------------------------
%% @doc Returns a fun that can be used for folding in calls to
%% couch_view:fold/4.
%%
%% It borrows from hovercraft.
%% ---------------------------------------------------------------------------

fold_reduce_view_acc_fun(Db, Group, GroupLevel) ->
    UpdateSeq = couch_db:get_update_seq(Db),
    CurrentEtag = couch_httpd_view:view_group_etag(Group, Db),
    Helpers = #reduce_fold_helper_funs{
      %%reduce_count=fun couch_view:reduce_to_count/1,
      start_response=fun doc_fold_reduce_start_response/4,
      send_row=fun doc_fold_reduce_send_row/3},
    couch_httpd_view:make_reduce_fold_funs(
        nil, GroupLevel, undefined, CurrentEtag, UpdateSeq, Helpers).

fold_view_acc_fun(Db, RowCount, Args) ->
    UpdateSeq = couch_db:get_update_seq(Db),
    Helpers = #view_fold_helper_funs{
      reduce_count=fun couch_view:reduce_to_count/1,
      start_response=fun doc_fold_start_response/6,
      send_row=fun doc_fold_send_row/5},
    couch_httpd_view:make_view_fold_fun(
      nil, Args, <<"">>, Db, UpdateSeq, RowCount, Helpers).

%% ---------------------------------------------------------------------------
%% @doc Returns a list of options that can be used with couch_view:fold/4.
%% ---------------------------------------------------------------------------

fold_view_options(#view_query_args{start_key=StartKey, 
                                   end_key=EndKey,
                                   direction=Dir}) ->
    lists:flatten([start_key(StartKey, Dir),
                   end_key(EndKey, Dir),
                   {dir, Dir}]).

start_key(undefined, _) -> [];
start_key(Key, Dir) -> {start_key, {Key, unbound_start(Dir)}}.

end_key(undefined, _) -> [];
end_key(Key, Dir) -> {end_key, {Key, unbound_end(Dir)}}.

%% ---------------------------------------------------------------------------
%% @doc Adds an _admin role user context if user_ctx isn't specified in
%% Options.
%% ---------------------------------------------------------------------------

maybe_add_admin_role(Options) ->
    case proplists:get_value(user_ctx, Options) of
        undefined ->
            [{user_ctx, #user_ctx{roles=[<<"_admin">>]}}|Options];
        _ ->
            Options
    end.

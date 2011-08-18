-module(couchlib_nqs).

-behaviour(gen_server).

-import(couchlib_doc, [strip_couch_obj/1]).

-export([start_link/0, prompt/2, set_timeout/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {funs=[], timeout=5000}).

-define(RESET,     <<"reset">>).
-define(ADD_FUN,   <<"add_fun">>).
-define(MAP_DOC,   <<"map_doc">>).
-define(REDUCE,    <<"reduce">>).
-define(REREDUCE,   <<"rereduce">>).

-define(ERROR(Msg), [<<"error">>, <<"native_query_server">>, 
                     list_to_binary(Msg)]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

prompt(Nqs, Data) ->
    gen_server:call(Nqs, {prompt, Data}).

set_timeout(Nqs, Timeout) ->
    gen_server:call(Nqs, {timeout, Timeout}).

handle_call({prompt, [?RESET, _]}, _From, State) ->
    {reply, true, State#state{funs=[]}};

handle_call({prompt, [?ADD_FUN, Bin]}, _From, #state{funs=Funs}=State) ->
    {reply, true, State#state{funs=[to_fun(Bin)|Funs]}};

handle_call({prompt, [?MAP_DOC, {Doc}]}, _From, #state{funs=Funs}=State) ->
    {reply, apply_maps(Funs, Doc, []), State};

handle_call({prompt, [?REDUCE, Funs, B]}, _From, State) ->
    {reply, apply_reduces(Funs, B), State};

handle_call({prompt, [?REREDUCE, Funs, B]}, _From, State) ->
    {reply, apply_rereduces(Funs, B), State};

handle_call({prompt, Data}, _From, State) ->
    error_logger:error_report({unhandled_prompt, Data}),
    {noreply, State};

handle_call({timeout, Timeout}, _From, State) ->
    {reply, ok, State#state{timeout=Timeout}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% @doc Converts a fun representation into an Erlang function. Funs can be
%% encoded as binary Erlang terms (term_to_binary) or as string source code.
%% ---------------------------------------------------------------------------

to_fun(B) when is_binary(B) ->
    try binary_to_term(B) of
        Term -> term_to_fun(Term)
    catch
        error:badarg -> to_fun(binary_to_list(B))
    end;

to_fun(S) when is_list(S) ->
    case erl_scan:string(S) of
        {ok, Tokens, _} ->
            case erl_parse:parse_exprs(Tokens) of
                {ok, [Parsed]} ->
                    try erl_eval:expr(Parsed, []) of
                        {value, Term, _} -> term_to_fun(Term)
                    catch
                        error:Err -> throw(Err)
                    end;
                {error, {Line, _Mod, [Msg, Params]}} ->
                    throw(lists:concat([Msg, Params, " on line ", Line]))
            end;
        {error, {Line, erl_scan, {string, _Char, Str}}, _Loc} ->
            throw(lists:concat(["Bad char on line ", Line, " at: ", Str]))
    end.

%% ---------------------------------------------------------------------------
%% @doc Returns a support fun type or throws {invalid_fun_spec, Term}.
%% ---------------------------------------------------------------------------

term_to_fun(F) when is_function(F) -> F;
term_to_fun({M, F}=T) when is_atom(M) andalso is_atom(F) -> T;
term_to_fun({M, F, A}=T) when is_atom(M) andalso 
                              is_atom(F) andalso 
                              is_list(A) -> T;
term_to_fun(Term) -> throw({invalid_fun_spec, Term}).

%% ---------------------------------------------------------------------------
%% @doc Applies a list of rereduce funs to values.
%% No Idea what happens next :P
%% ---------------------------------------------------------------------------

apply_rereduces(Funs, B) ->
    apply_rereduces(Funs, B, []).
apply_rereduces([], _B, Acc) -> [true, Acc];
apply_rereduces([FDef|T], B, Acc) ->
    apply_rereduces(T, B, [apply_rereduce(FDef, B)|Acc]).

apply_rereduce(FDef, B) ->
    apply_rereduce(FDef, B, []).
apply_rereduce(FDef, B, Acc) when is_binary(FDef) ->
    apply_rereduce(to_fun(FDef), B, Acc);
apply_rereduce(FDef, [Val|T], Values) when is_function(FDef)->
    apply_rereduce(FDef, T, [Val|Values]);
apply_rereduce(FDef, [], Values) ->
    FDef([],Values,true).

%% ---------------------------------------------------------------------------
%% @doc Applies a list of reduce funs to Key values.
%% No Idea what happens next :P
%% ---------------------------------------------------------------------------

apply_reduces(Funs, B) ->
    apply_reduces(Funs, B, []).
apply_reduces([], _B, Acc) -> [true, Acc];
apply_reduces([FDef|T], B, Acc) ->
    apply_reduces(T, B, [apply_reduce(FDef, B)|Acc]).

apply_reduce(FDef, B) ->
    apply_reduce(FDef, B, {[],[]}).
apply_reduce(FDef, B, Acc) when is_binary(FDef) ->
    apply_reduce(to_fun(FDef), B, Acc);
apply_reduce(FDef, [[Key|Val]|T], {Keys, Values}) when is_function(FDef)->
    apply_reduce(FDef, T, {[Key|Keys], [Val|Values]});
apply_reduce(FDef, [], {Keys, Values}) ->
    FDef(Keys,Values,false).

%% ---------------------------------------------------------------------------
%% @doc Applies a list of maps to a doc. This reverses the list of mapped
%% results by design.
%% ---------------------------------------------------------------------------

apply_maps([], _, Acc) -> Acc;
apply_maps([FDef|T], Doc, Acc) ->
    apply_maps(T, Doc, [apply_map(FDef, Doc)|Acc]).

%% ---------------------------------------------------------------------------
%% @doc Applies a function to a document map operation.
%% ---------------------------------------------------------------------------

apply_map(F, Doc) ->
    try apply_f(F, strip_couch_obj(Doc)) of
        Out -> 
            to_couch_map(Out, [])
    catch
        _:Err -> throw({map_error, {Err, erlang:get_stacktrace()}})
    end.

apply_f({M, F}, Doc) ->
    apply(M, F, [Doc]);
apply_f({M, F, A}, Doc) when is_list(A) -> 
    apply(M, F, [Doc] ++ A);
apply_f(F, Doc) when is_function(F) ->
    F(Doc).

%% ---------------------------------------------------------------------------
%% @doc Converts {Key, Val} tuples into [Key, Val] and single key values into
%% [Key, null].
%% ---------------------------------------------------------------------------

to_couch_map([], Acc) -> Acc;
to_couch_map([{K, V}|T], Acc) ->
    to_couch_map(T, [[K, V]|Acc]);
to_couch_map([K|T], Acc) ->
    to_couch_map(T, [[K, null]|Acc]).

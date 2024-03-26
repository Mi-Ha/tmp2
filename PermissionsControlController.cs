using Newtonsoft.Json;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Xml;
using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using DevExpress.CodeParser;
using DevExpress.DashboardExport.Map;
using System.Text.RegularExpressions;
using DevExpress.Data.Helpers;
using DevExpress.XtraRichEdit.Commands;
using ADWrapper;
using WebDAL;
using DevExpress.DashboardCommon;
using EmasMotesCore.Models.Models_EMAS;

namespace EmasMotesCore.Controllers
{
    public class PermissionsControlController : ControllerBase
    {
        private readonly EMAS_Context _db;
        private readonly WebPluginUserActionsService _webPluginUserActionsService;
        private readonly EventCriticalityTypeService _eventCriticalityTypeService;
        private readonly WebPluginsService _webPluginsService;
        private readonly UsersService _usersService;
        private readonly IConfiguration _configuration;

        // Мапка кубов с дименшенами из XML (не объекты доступа)
        Dictionary<String, Dictionary<String, String>> dictDimensionByCubeXML = null;
        // Мапка виртуальных кубов с дименшенами из XML (не объекты доступа)
        Dictionary<String, Dictionary<String, String>> dictDimensionByVirtualCubeXML = null;
        //Мапка объектов доступа, которые в наличии в XMLROLE
        Dictionary<String, Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>> dictObjAccessByRoleXMLROL = null;
        //Мапка дименшенов с аннотациями
        Dictionary<String, String> dictDimensionWithAnnotation = null;
        //Мапка Роль - Иерархия - Мембер
        Dictionary<String, Dictionary<String, Dictionary<String, bool>>> dictMemberByHierarhyByRoleXMLROL = null;
        //Мапка - хранилище изменений
        static Dictionary<string, Dictionary<string, string>> PermissionDataStorage = new Dictionary<string, Dictionary<string, string>>();

        public PermissionsControlController(EMAS_Context db, WebPluginUserActionsService webPluginUserActionsService, EventCriticalityTypeService eventCriticalityTypeService, WebPluginsService webPluginsService, UsersService usersService, IConfiguration configuration)
        {
            _db = db;
            _webPluginUserActionsService = webPluginUserActionsService;
            _eventCriticalityTypeService = eventCriticalityTypeService;
            _webPluginsService = webPluginsService;
            _usersService = usersService;
            _configuration = configuration;
        }

        public object PermissionsControlInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _db.Roles.ToList();
                return DataSourceLoader.Load(model, loadOptions);
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> RolesInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _db.Roles.Select(f => new RoleDTO { Id = f.Id, Name = f.Name, Description = f.Description });
                return Json(await DataSourceLoader.LoadAsync(model, loadOptions));

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> ReportsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                return Json(await DataSourceLoader.LoadAsync(
                    from r in _db.Reports
                    select new ReportDTO
                    {
                        Id = r.Id,
                        Name = r.Name
                    }
                    , loadOptions));

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> WebPluginsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _db.Webplugins.Select(wp => new WebPluginDTO
                {
                    Id = wp.Id,
                    Parentid = wp.Parentid,
                    Caption = wp.Caption,
                    Name = wp.Name,
                    Viewname = wp.Viewname,
                    Image = wp.Image,
                    Sortorder = wp.Sortorder,
                    Ismenubar = wp.Ismenubar,
                    Isnavigationbar = wp.Isnavigationbar,
                    Issecret = wp.Issecret,
                    Trial = wp.Trial,
                    Isadmin = wp.Isadmin,
                    Updatedate = wp.Updatedate,
                });


                return Json(await DataSourceLoader.LoadAsync(model, loadOptions));
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public object SdsParentsWebplugins(DataSourceLoadOptions loadOptions)
        {
            List<WebPluginDTO> model = new List<WebPluginDTO>();
            try
            {

                foreach (var wp in _db.Webplugins
                    //  .Where(o => o.Parentid == 1)
                    //       .OrderBy(o => o.Name))
                    .OrderBy(o => o.Caption))
                {
                    int PID = -1;
                    if (!(wp.Parentid is null))
                        PID = (int)wp.Parentid;
                    model.Add(new WebPluginDTO
                    {
                        Id = wp.Id,
                        Parentid = PID,
                        Caption = wp.Caption,
                        Name = wp.Name,
                        Viewname = wp.Viewname
                    });
                }
            }
            catch (Exception ex)
            {
                //     logger.Error(ex);
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }
        public object GetWebPluginsList(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _webPluginsService.GetWebPluginsList().OrderBy(o => o.Caption);
                return DataSourceLoader.Load(model, loadOptions);
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public object GetWebPluginsUserActionsList(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _webPluginUserActionsService.GetWebPluginsUserActionsList().OrderBy(f => f.Name);
                return DataSourceLoader.Load(model, loadOptions);

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public object GetEventCriticalityTypesList(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _eventCriticalityTypeService.GetEventCriticalityTypeList().OrderBy(o => o.Name);
                return DataSourceLoader.Load(model, loadOptions);
            }
            catch (Exception ex)
            {
                //     logger.Error(ex);
                return BadRequest(null, ex);
            }
        }

        [HttpPost]
        public IActionResult RoleInsert(string values)
        {
            var newRole = new Role();
            JsonConvert.PopulateObject(values, newRole);
            try
            {

                _db.Roles.Add(newRole);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult RoleUpdate(int key, string values)
        {
            try
            {
                var editRoles = _db.Roles.First(p => p.Id == key);
                if (editRoles != null)
                {
                    JsonConvert.PopulateObject(values, editRoles);
                    _db.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object RoleDelete(int key)
        {
            try
            {
                var unit = _db.Roles.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.Roles.Remove(unit);
                    _db.SaveChanges();
                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        [HttpPost]
        public IActionResult WebPluginInsert(string values)
        {
            var newWebPlugin = new Webplugin();
            JsonConvert.PopulateObject(values, newWebPlugin);

            try
            {

                _db.Webplugins.Add(newWebPlugin);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult WebPluginUpdate(int key, string values)
        {
            try
            {
                var editWebplugins = _db.Webplugins.First(p => p.Id == key);
                if (editWebplugins != null)
                {
                    JsonConvert.PopulateObject(values, editWebplugins);

                    _db.SaveChanges();

                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object WebPluginDelete(int key)
        {
            try
            {
                var unit = _db.Webplugins.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.Webplugins.Remove(unit);
                    _db.SaveChanges();

                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> RolesWebPluginsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _webPluginsService.GetModel();
                return Json(await DataSourceLoader.LoadAsync(model, loadOptions));
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        [HttpPost]
        public IActionResult RolesWebPluginsInsert(string values)
        {
            try
            {
                EmasResult result = _webPluginsService.InsertElementWithResult(values);
                if ((result.Type != EmasResultType.Success || result.Type != EmasResultType.Empty) && result.ShowMessage)
                {
                    return new ConflictObjectResult(new { message = result.Message });
                }
            }
            catch(Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }

        [HttpPut]
        public IActionResult RolesWebPluginsUpdate(int key, string values)
        {
            try
            {
                EmasResult result = _webPluginsService.UpdateElementWithResult(key, values);
                if ((result.Type != EmasResultType.Success || result.Type != EmasResultType.Empty) && result.ShowMessage)
                {
                    return new ConflictObjectResult(new { message = result.Message });
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }

        [HttpDelete]
        public async Task<object> RolesWebPluginsDelete(int key)
        {
            var rolesWebPlugins = await _webPluginsService.GetModel().FirstOrDefaultAsync(p => p.Id == key);

            if (rolesWebPlugins != null && rolesWebPlugins.Id > 0)
            {
                _webPluginsService.DeleteElement(key);

            }
            return Ok();
        }

        public async Task<IActionResult> ReportsRolesInit(DataSourceLoadOptions loadOptions)
        {
            try
            {

                return Json(await DataSourceLoader.LoadAsync(
                    from rr in _db.Reportsroles
                    select
                    new ReportsRolesDTO
                    {
                        Id = rr.Id,
                        Reportid = rr.Reportid,
                        Roleid = rr.Roleid
                    },
                    loadOptions));
            }
            catch (Exception ex)
            {
                //    logger.Error(ex);
                return BadRequest(null, ex);
            }
        }
        [HttpPost]
        public IActionResult ReportsRolesInsert(string values)
        {

            var newReportsroles = new Reportsrole();
            JsonConvert.PopulateObject(values, newReportsroles);
            try
            {

                _db.Reportsroles.Add(newReportsroles);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult ReportsRolesUpdate(int key, string values)
        {
            try
            {
                var editReportsroles = _db.Reportsroles.First(p => p.Id == key);
                if (editReportsroles != null)
                {
                    JsonConvert.PopulateObject(values, editReportsroles);
                    _db.SaveChanges();

                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object ReportsRolesDelete(int key)
        {
            try
            {
                var unit = _db.Reportsroles.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.Reportsroles.Remove(unit);
                    _db.SaveChanges();

                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        public async Task<IActionResult> DashboardsRolesInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                return Json(await DataSourceLoader.LoadAsync(
                    from dr in _db.DashRolesdashboards
                    select new DashRoleDashboardDTO
                    {
                        Id = dr.Id,
                        DashboardId = dr.Dashboardid,
                        RoleId = dr.Roleid,
                    }
                    , loadOptions));

            }
            catch (Exception ex)
            {
                //    logger.Error(ex);
                return BadRequest(null, ex);
            }
        }
        [HttpPost]
        public IActionResult DashboardsRolesInsert(string values)
        {
            var newDashboardRole = new DashRolesdashboard();
            JsonConvert.PopulateObject(values, newDashboardRole);
            try
            {

                _db.DashRolesdashboards.Add(newDashboardRole);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult DashboardsRolesUpdate(int key, string values)
        {
            try
            {
                var editDashboardRole = _db.DashRolesdashboards.First(p => p.Id == key);
                if (editDashboardRole != null)
                {
                    JsonConvert.PopulateObject(values, editDashboardRole);

                    _db.SaveChanges();

                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object DashboardsRolesDelete(int key)
        {
            try
            {
                var unit = _db.DashRolesdashboards.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.DashRolesdashboards.Remove(unit);
                    _db.SaveChanges();

                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        public async Task<IActionResult> DashboardsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                return Json(await DataSourceLoader.LoadAsync(
                    from dash in _db.DashDashboards
                    select new DashboardNameDTO
                    {
                        Id = dash.Id,
                        Caption = dash.Caption,
                    }
                    , loadOptions));
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        public object LookupWebPlugins(DataSourceLoadOptions loadOptions)
        {
            List<LookupItem> model = new List<LookupItem>();
            try
            {
                string strQuery = "SELECT ID, coalesce(ParentID, '-1') AS ParentID, Name, Caption, " +
                                  " ComponentPath, Image, coalesce(IsMenuBar, false) IsMenuBar, " +
                                  " coalesce(IsNavigationBar, false) IsNavigationBar, coalesce(IsToolBar,false) IsToolBar," +
                                  " coalesce(IsSecret, false) IsSecret, SortOrder, Trial,UpdateDate, IsAdmin, " +
                                  " Caption_Eng FROM WebPlugins ORDER BY Caption";
                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandType = CommandType.Text;
                    sql.CommandText = strQuery;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    while (reader.Read())
                    {
                        model.Add(new LookupItem { Id = long.Parse(reader["ID"].ToString()), Name = reader["Caption"].ToString() });
                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }

        public object WebPluginIndicatorInit(DataSourceLoadOptions loadOptions)
        {
            List<WebPluginIndicator> model = new List<WebPluginIndicator>();
            try
            {
                string strQuery = "SELECT ID, Name, Caption, WebPluginId, IsActive FROM WebPluginIndicators ORDER BY WebPluginId, ID ";

                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandType = CommandType.Text;
                    sql.CommandText = strQuery;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    while (reader.Read())
                    {
                        WebPluginIndicator item = new WebPluginIndicator();
                        item.Id = long.Parse(reader["ID"].ToString());
                        item.Webpluginid = long.Parse(reader["Webpluginid"].ToString());
                        item.Name = reader["Name"].ToString();
                        item.Caption = reader["Caption"].ToString();
                        item.Isactive = reader["IsActive"] == DBNull.Value ? false : Convert.ToBoolean(reader["IsActive"]);
                        model.Add(item);
                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }

        public object WebPluginIndicatorInsert(string values)
        {
            WebPluginIndicator item = new WebPluginIndicator();
            JsonConvert.PopulateObject(values, item);

            try
            {

                _db.WebPluginIndicators.Add(item);
                _db.SaveChanges();

            }
            catch (Exception e)
            {
                return BadRequest(null, e);
            }
            return Ok();
        }
        public IActionResult WebPluginIndicatorUpdate(int key, string values)
        {
            try
            {
                var item = _db.WebPluginIndicators.First(p => p.Id == key);
                if (item != null)
                {
                    JsonConvert.PopulateObject(values, item);

                    _db.SaveChanges();

                }
            }
            catch (Exception e)
            {
                return BadRequest(null, e);
            }
            return Ok();
        }

        public IActionResult WebPluginIndicatorDelete(int key)
        {
            try
            {
                var item = _db.WebPluginIndicators.First(p => p.Id == key);
                if (item != null && item.Id > 0)
                {
                    _db.WebPluginIndicators.Remove(item);
                    _db.SaveChanges();

                }
            }
            catch (Exception e)
            {
                return BadRequest(null, e);
            }
            return Ok();
        }

        public object GetUsers(DataSourceLoadOptions loadOptions)
        {
            List<LookupItem> model = new List<LookupItem>();
            try
            {
                string strQuery = "SELECT coalesce(u.LastName,'') || ' ' || coalesce(u.FirstName,'') || ' ' || coalesce(u.MiddleName,'') || ', ' || u.Login as Login " +
                                  " FROM Users u where IsActive = true AND u.Login is NOT NULL and u.Login <> '' ORDER BY u.LastName";

                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandType = CommandType.Text;
                    sql.CommandText = strQuery;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    int id = 1;
                    while (reader.Read())
                    {
                        model.Add(new LookupItem { Id = id, Name = reader["Login"].ToString() });
                        id++;
                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }

        [HttpPost]
        public async Task<IActionResult> UserChanged(string userLogin)
        {
            //await HttpContext.SignOutAsync(
            //   CookieAuthenticationDefaults.AuthenticationScheme);
            if (ModelState.IsValid)
            {            
                ApplicationUser user = null;
                var _dbUser = _usersService.GetUser(u => u.Login.ToLower() == userLogin.Substring(userLogin.LastIndexOf(",") + 2).ToLower() &&
                                     u.Isactive == true
                                     );
                if (_dbUser != null)
                {
                    var oldUser = HttpContext.Session.GetObjectFromJson<long>("UserID");
                    user = new ApplicationUser() { Name = _dbUser.Login, FullName = _dbUser.FullName, RoleId = _dbUser.Roleid, UserId = _dbUser.Id };
                    HttpContext.Session.SetObjectAsJson("UserID", user.UserId);
                    HttpContext.Session.SetObjectAsJson("UserId", user.UserId);
                    HttpContext.Session.SetObjectAsJson("RoleId", user.RoleId);
                    HttpContext.Session.SetObjectAsJson("UserLogin", user.Name);
                    HttpContext.Session.SetObjectAsJson("UserFullName", user.FullName);
                    HttpContext.Session.SetObjectAsJson("IsSwitched", true);
                    AddJournalDb_EMAS(UserAction.System_login, "Вход в систему пользователя: ID = " + oldUser + " под ID = " + HttpContext.Session.GetObjectFromJson<long>("UserID"), null, "EventsLog");
                }

                //var claims = new List<Claim>
                //{
                //    new Claim(ClaimTypes.Name, user.Name),
                //    new Claim(ClaimTypes.Role, "Administrator"),
                //};

                //var claimsIdentity = new ClaimsIdentity(
                //    claims, CookieAuthenticationDefaults.AuthenticationScheme);

                //var authProperties = new AuthenticationProperties
                //{ };

                //await HttpContext.SignInAsync(
                //    CookieAuthenticationDefaults.AuthenticationScheme,
                //    new ClaimsPrincipal(claimsIdentity),
                //    authProperties);

                return Ok();
            }

            // Something failed. Redisplay the form.
            // using static EmasMotesCore.Account.LoginModel;
            EmasMotesCore.Account.LoginModel model = new EmasMotesCore.Account.LoginModel() { Name = "", Password = "" };
            return View("Login", model);
        }


        //Список ролей из базы данных
        private List<Role> getRoleList()
        {

            List<Role> model = new List<Role>();
            try
            {
                model = _db.Roles.OrderBy(p => p.Name).ToList();
                //model = _db.Roles.Where(p => p.Name == "Guest").OrderBy(p => p.Name).ToList();
                //model = _db.Roles.Where(p => p.Name == "Администраторы EMAS").OrderBy(p => p.Name).ToList();
                //model = _db.Roles.Where(p => p.Name == "Администраторы EMAS" || p.Name == "Guest").OrderBy(p => p.Name).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка загрузки таблицы roles");
            }
            return model;

        }

        //Список станций
        private List<Station> getStationList()
        {
            List<Station> model = new List<Station>();
            try
            {
                model = _db.Stations.OrderBy(p => p.Name).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка загрузки таблицы stations");
            }
            return model;
        }

        private List<Stationrole> getStationRoleList()
        {

            List<Stationrole> listStationrole = null;
            try
            {

                var query = from sr in _db.Stationroles
                            join r in _db.Roles on sr.Roleid equals r.Id
                            join st in _db.Stations on sr.Stationid equals st.Id
                            select new Stationrole
                            {
                                Id = sr.Id,
                                Roleid = r.Id,
                                Stationid = st.Id,
                                Role = new Role { Id = r.Id, Name = r.Name },
                                Station = new Station { Id = st.Id, Name = st.Name }

                            };

                listStationrole = query.ToList();

            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка загрузки таблицы stationroles");
            }
            return listStationrole;
        }


        //Мапка "Роли по станциям"
        private Dictionary<string, Dictionary<string, string>> getStationRoleDict()
        {

            Dictionary<string, Dictionary<string, string>> model = new Dictionary<string, Dictionary<string, string>>();

            List<Stationrole> listStationrole = getStationRoleList();

            foreach (Stationrole itemStationrole in listStationrole)
            {

                if (!model.ContainsKey(itemStationrole.Station.Name))
                {
                    Dictionary<string, string> newDictRole = new Dictionary<string, string>();
                    newDictRole.Add(itemStationrole.Role.Name, itemStationrole.Role.Name);
                    model.Add(itemStationrole.Station.Name, newDictRole);
                }
                else
                {
                    Dictionary<string, string> dictRole = model[itemStationrole.Station.Name];
                    dictRole.Add(itemStationrole.Role.Name, itemStationrole.Role.Name);
                }

            }

            return model;
        }


        public object OlapAccessGridColumnInit(string mode, DataSourceLoadOptions loadOptions)  //isCube
        {
            List<Role> listRole = getRoleList();
            List<DataGridColumns> result = new List<DataGridColumns>();
            String txtCaption = "";

            switch (mode)
            {
                case "cube":
                    HttpContext.Session.Remove("CubeOlapAccessGUID");
                    HttpContext.Session.SetObjectAsJson("CubeOlapAccessGUID", Guid.NewGuid().ToString());
                    txtCaption = "Куб/Роль";
                    break;
                case "dimension":
                    HttpContext.Session.Remove("DimensionOlapAccessGUID");
                    HttpContext.Session.SetObjectAsJson("DimensionOlapAccessGUID", Guid.NewGuid().ToString());
                    txtCaption = "Измерение/Роль";
                    break;
                case "rolestation":
                    HttpContext.Session.Remove("RoleStationOlapAccessGUID");
                    HttpContext.Session.SetObjectAsJson("RoleStationOlapAccessGUID", Guid.NewGuid().ToString());
                    txtCaption = "Станция/Роль";
                    break;
            }

            result.Add(new DataGridColumns() { DataField = "OlapName", Caption = txtCaption, DataType = GridColumnDataType.String, Fixed = true, ReadOnly = false, Visible = true });

            //Add columns with roles
            try
            {
                foreach (Role item in listRole)
                {
                    result.Add(new DataGridColumns() { DataField = roleIdGenerate(item.Name), Caption = item.Name, DataType = GridColumnDataType.Boolean, Fixed = false, ReadOnly = false, Visible = true });
                }
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.ExceptionErrorMessage() + "  && OlapAccessGridColumnInit, 1" });
            }

            switch (mode)
            {
                case "cube": return PartialView("PermissionsControlCubeGrid", result);
                case "dimension": return PartialView("PermissionsControlDimensionGrid", result);
                case "rolestation": return PartialView("PermissionsControlRolesStationsGrid", result);
            }
            return new DataTable();
        }

        private string roleIdGenerate(string roleName)
        {
            return roleName.Replace(".", "_a26da675_f228_4360_fff_").Replace(" ", "_87cd_4a9e753cf5db_");
        }

        private string getShemaFilePath()
        {
            string? filePath = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPschemaPath")?.Value;
            return filePath;
        }
        private string getShemaFilePathWithRoot()
        {
            string? filePath = HttpContext.Session.GetObjectFromJson<string>("ShemaFileSelected");
            if(filePath == null ) {
                filePath = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPschemaPath")?.Value;
            }

            string? filePathRoot = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAP_RootPatch")?.Value;
            if(filePathRoot == null)
            {
               throw new Exception("Не удалось получить из базы данных корневой путь к OLAP-файлам && getShemaFilePath, 1"); 
            }


            return filePathRoot+filePath;
        }

        public object GetShemaFilePathListBD(DataSourceLoadOptions loadOptions)
        {
            Dictionary<long, string> model = _db.VSystemsettings.Select(input => input) //s => new{ s.Id, s.Value, s.Name } )
                .Distinct().Where(s => s.Name == "OLAPschemaPathM")
                .OrderBy(input => input.Id)
                .ToDictionary(x => (long)x.Id, x => x.Value)
                .OrderBy(i=>i.Key)
                .Select((entry, i) => new { entry.Value, i })
                .ToDictionary(pair=>(long)pair.i, pair=>pair.Value);

            return DataSourceLoader.Load(model, loadOptions);
        }

        public object GetShemaFilePathList(DataSourceLoadOptions loadOptions)
        {
            string? filePathRoot = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAP_RootPatch")?.Value;
            if(filePathRoot == null)
            {
               throw new Exception("Не удалось получить из базы данных корневой путь к OLAP-файлам && GetShemaFilePathList, 1"); 
            }

            string? filePath = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAP_DataSources")?.Value;

            if(filePath == null)
            {
               throw new Exception("Не удалось получить из базы данных путь к файлу DataSource.xml  && GetShemaFilePathList, 2"); 
            }

            String strDataSourceFile = "";
            MondrianDataSources objMondrianDataSources = null;
            filePath = filePathRoot + filePath;

            try
            {
                strDataSourceFile = XDocument.Load(filePath).ToString();
            }
            catch (Exception ex)
            {
                throw new Exception("Не найден файл " + filePath + "  && GetShemaFilePathList, 3");
            }

            try
            {
                XmlRootAttribute xmlRoot = new XmlRootAttribute
                {
                    ElementName = "DataSources",
                    IsNullable = true
                };

                XmlSerializer serializer = new XmlSerializer(typeof(MondrianDataSources), xmlRoot);


                using (StringReader reader = new StringReader(strDataSourceFile))
                {
                    objMondrianDataSources = (MondrianDataSources)serializer.Deserialize(reader);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && GetShemaFilePathList, 4");
            }

            Dictionary<long, string> model = new Dictionary<long, string>();
            long cnt = 0;
            
            foreach(DataSourcesDataSourceCatalog item in objMondrianDataSources.DataSource.Catalogs)
            {
                string strDef = item.Definition;
                model.Add(cnt++, strDef);
            }

            return DataSourceLoader.Load(model, loadOptions);

        }
        private string getUserName_OlapServerRestart()
        {

            string value = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPserverRestartUserName").Value;
            return value;

        }
        private string getPassword_OlapServerRestart()
        {
            string value = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPserverRestartPassword").Value;
            return value;
        }
        private string getUrl_OlapServerRestart()
        {
            string value = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPserverRestartUrl").Value;
            return value;
        }
        private Schema getShema()
        {
            string filePath = getShemaFilePath();
            //string filePath = getShemaFilePathWithRoot();  //С селектором схем

            String strSchemaFile = "";
            Schema objSchema = null;

            try
            {
                strSchemaFile = XDocument.Load(filePath).ToString();
            }
            catch (Exception ex)
            {
                throw new Exception("Не найден файл " + filePath + "  && getShema, 1");
            }

            try
            {
                XmlRootAttribute xmlRoot = new XmlRootAttribute
                {
                    ElementName = "Schema",
                    IsNullable = true
                };

                XmlSerializer serializer = new XmlSerializer(typeof(Schema), xmlRoot);


                using (StringReader reader = new StringReader(strSchemaFile))
                {
                    objSchema = (Schema)serializer.Deserialize(reader);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && getShema, 2");
            }

            foreach (var role in objSchema.Role)
            {
                foreach (var sg in role.SchemaGrant)
                {
                    sg.CubeGrant = sg.CubeGrant.ToList().GroupBy(cg => cg.cube).Select(g => g.First()).ToArray();
                }
            }

            return objSchema;
        }

        //Получение данных для таблицы
        public object OlapAccessGridInitData(string mode, DataSourceLoadOptions loadOptions)
        {
            DataTable result = new DataTable();
            try
            {
                switch (mode)
                {
                    case "cube":
                        result = GetCubeGridData();
                        break;
                    case "dimension":
                        result = GetDimensionGridData();
                        break;
                    case "rolestation":
                        result = GetRolestationGridData();
                        break;
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }

            IEnumerable<Dictionary<string, object>> model = ToDataTableList(result);
            return DataSourceLoader.Load(model, loadOptions);
        }

        public object OlapAccessGridInitDataM(string mode, string shemafile, DataSourceLoadOptions loadOptions)
        {
            HttpContext.Session.SetObjectAsJson("ShemaFileSelected", shemafile);
            return OlapAccessGridInitData(mode, loadOptions);
        }

        //Получить мапку ролей из схемы
        public Dictionary<String, SchemaRole> getMapSchemaRole(Schema objSchema)
        {
            Dictionary<String, SchemaRole> mapRole = new Dictionary<String, SchemaRole>();

            if (objSchema == null) return mapRole;
            if (objSchema.Role == null) return mapRole;

            foreach (SchemaRole item in objSchema.Role.ToList())
            {
                mapRole.Add(item.name, item);
            }
            return mapRole;
        }

        //Получить мапку ролей из БД
        public Dictionary<String, Role> getMapRoleBD(List<Role> listRole)
        {
            Dictionary<String, Role> mapRole = new Dictionary<String, Role>();

            if (listRole == null) return mapRole;

            foreach (Role item in listRole)
            {
                mapRole.Add(item.Name, item);
            }
            return mapRole;
        }

        //Заполнение мапки dictObjAccessByRoleXMLROL объектами доступа из схемы XMLROLE
        //(до кубов включительно)
        private int fillDictObjAccessByRoleXMLROL(Schema objSchema)
        {
            int countCheck = 0;

            dictObjAccessByRoleXMLROL = new Dictionary<String, Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>>();

            if (objSchema.Role == null) return countCheck;
            List<SchemaRole> lstShemRole = objSchema.Role.ToList();

            //Список дименшенов (иерархий), у которых есть аннотации
            Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);

            foreach (SchemaRole itemShemRole in lstShemRole)
            {

                if (itemShemRole.SchemaGrant[0] == null) continue;
                if (itemShemRole.SchemaGrant[0].CubeGrant == null) continue;

                Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> dictCubeXMLROL = new Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>();
                dictObjAccessByRoleXMLROL.Add(itemShemRole.name, dictCubeXMLROL);

                foreach (SchemaRoleSchemaGrantCubeGrant itemCube in itemShemRole.SchemaGrant[0].CubeGrant.ToList())
                {

                    if (itemCube.HierarchyGrant == null) continue;
                    Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant> dictHierarchyXMLROL = new Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                    if (!dictCubeXMLROL.ContainsKey(itemCube.cube))
                    {
                        dictCubeXMLROL.Add(itemCube.cube, dictHierarchyXMLROL);
                    }
                }
            }

            return countCheck;
        }


        private SchemaRoleSchemaGrantCubeGrantHierarchyGrant createHierarhyForDictObjAccessByRoleXMLROL(String hierarhy
                                                                    , SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess hierarhyAccess
                                                                    , Dictionary<String, bool> memberBase
                                                                    , SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant[] memberGrantRole
                                                                    )
        {

            SchemaRoleSchemaGrantCubeGrantHierarchyGrant objHierarchyXMLROL = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
            {
                hierarchy = hierarhy,
                rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial,
                access = hierarhyAccess
            };

            if (hierarhyAccess == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
            {

                if (memberBase != null)
                {

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listObjMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach (KeyValuePair<String, bool> entry in memberBase)
                    {

                        SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                        {
                            member = entry.Key,
                            access = (entry.Value == true) ? SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all
                                                            : SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none
                        };
                        listObjMember.Add(objMember);
                    }
                    objHierarchyXMLROL.MemberGrant = listObjMember.ToArray();
                }
                else if (memberGrantRole != null)
                {

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listObjMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMember in memberGrantRole.ToList())
                    {
                        SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                        {
                            member = itemMember.member,
                            access = itemMember.access
                        };
                        listObjMember.Add(objMember);
                    }
                    objHierarchyXMLROL.MemberGrant = listObjMember.ToArray();
                }
            }

            return objHierarchyXMLROL;
        }

        Dictionary<String, bool> dictCubeVisible = new  Dictionary<String, bool>();

        //Заполнение мапки с кубами и дименшенами из XML (не объекты доступа)
        private void filldictDimensionByCubeXML(Schema objSchema)
        {

            if (dictDimensionByCubeXML != null) return;
            dictDimensionByCubeXML = new Dictionary<String, Dictionary<String, String>>();
            if( objSchema.Cube == null ) return;

            foreach (SchemaCube itemCub in objSchema.Cube.ToList())
            {
                if(dictCubeVisible.ContainsKey(itemCub.name))
                {
                    if (itemCub.visible == false)
                        dictCubeVisible[itemCub.name] = false;
                    else
                        dictCubeVisible[itemCub.name] = true;
                }
                else
                {
                    if (itemCub.visible == false)
                        dictCubeVisible.Add(itemCub.name, false);
                    else
                        dictCubeVisible.Add(itemCub.name, true);
                }
                if (itemCub.visible == false) continue; //Оставляем только видимые кубы

                Dictionary<String, String> dictDim = new Dictionary<String, String>();

                for (int ind = 0; ind < itemCub.Items.Length; ind++)
                {
                    dictDim.Add(((DimensionUsage)itemCub.Items[ind]).name, ((DimensionUsage)itemCub.Items[ind]).name);
                }
                dictDimensionByCubeXML.Add(itemCub.name, dictDim);
            }
            return;
        }

        //Заполнение мапки с виртуальными кубами и дименшенами из XML (не объекты доступа)
        private void filldictDimensionByVirtualCubeXML(Schema objSchema)
        {

            if (dictDimensionByVirtualCubeXML != null) return;
            
            dictDimensionByVirtualCubeXML = new Dictionary<String, Dictionary<String, String>>();

            if (objSchema.VirtualCube == null) return;
            foreach (SchemaVirtualCube itemVCub in objSchema.VirtualCube.ToList())
            {
                //if (itemVCub.visible == false) continue; //Оставляем только видимые кубы
                dictCubeVisible.Add(itemVCub.name, true);

                Dictionary<String, String> dictDim = new Dictionary<String, String>();
                List<SchemaVirtualCubeVirtualCubeDimension> listVDim = itemVCub.VirtualCubeDimension.ToList();

                foreach (SchemaVirtualCubeVirtualCubeDimension itemVDim in listVDim)
                {
                    dictDim.Add(itemVDim.name, itemVDim.name);
                }
                dictDimensionByVirtualCubeXML.Add(itemVCub.name, dictDim);
            }

            return;
        }

        private SchemaRole createNewSchemaRole(string roleName)
        {

            SchemaRoleSchemaGrant objSchemaRoleSchemaGrant = new SchemaRoleSchemaGrant { access = SchemaRoleSchemaGrantAccess.all };
            SchemaRole objSchemaRole = new SchemaRole { name = roleName };
            List<SchemaRoleSchemaGrant> listSchemaRoleSchemaGrant = new List<SchemaRoleSchemaGrant>() { objSchemaRoleSchemaGrant };
            objSchemaRole.SchemaGrant = listSchemaRoleSchemaGrant.ToArray();
            return objSchemaRole;
        }

        // Проверка целостности кубов в роли
        private int checkCubeXMLROL(Schema objSchema, ref SchemaRole schemaRoleXMLROL)
        {

            int countCheck = 0;
            Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> dictObjAccessCube = dictObjAccessByRoleXMLROL[schemaRoleXMLROL.name];

            if (schemaRoleXMLROL.SchemaGrant == null)
            {
                SchemaRoleSchemaGrant objSchemaRoleSchemaGrant = new SchemaRoleSchemaGrant();
                objSchemaRoleSchemaGrant.access = SchemaRoleSchemaGrantAccess.all;
                List<SchemaRoleSchemaGrant> listSchemaRoleSchemaGrant = new List<SchemaRoleSchemaGrant>() { objSchemaRoleSchemaGrant };
                schemaRoleXMLROL.SchemaGrant = listSchemaRoleSchemaGrant.ToArray();
            }

            List<SchemaRoleSchemaGrantCubeGrant> listCubeXMLROL = null;
            if (schemaRoleXMLROL.SchemaGrant[0].CubeGrant == null)
            {
                listCubeXMLROL = new List<SchemaRoleSchemaGrantCubeGrant>();
                schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();
            }
            else
            {
                listCubeXMLROL = schemaRoleXMLROL.SchemaGrant[0].CubeGrant.ToList();
            }

            //Проверка кубов - удаляем лишнее
            if (listCubeXMLROL.Count > 0)
            {
                List<SchemaRoleSchemaGrantCubeGrant> listCubeXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrant>();

                //Формируем список на удаление
                foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXMLROL in listCubeXMLROL)
                {
                    if (!dictDimensionByCubeXML.ContainsKey(itemCubeXMLROL.cube) && !dictDimensionByVirtualCubeXML.ContainsKey(itemCubeXMLROL.cube))
                    {
                        listCubeXMLROLforDelete.Add(itemCubeXMLROL);
                    }
                }
                //Удаляем
                foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXMLROL in listCubeXMLROLforDelete)
                {
                    listCubeXMLROL.Remove(itemCubeXMLROL);

                    if (dictObjAccessCube.ContainsKey(itemCubeXMLROL.cube))
                    {
                        dictObjAccessCube.Remove(itemCubeXMLROL.cube);
                        countCheck++;
                    }
                }
                schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();
            }

            //Добавляем
            //Кубы
            listCubeXMLROL = schemaRoleXMLROL.SchemaGrant[0].CubeGrant.ToList();
            foreach (KeyValuePair<String, Dictionary<String, String>> entry in dictDimensionByCubeXML)
            {

                SchemaRoleSchemaGrantCubeGrant objCubeXMLROL = null;
                if (!dictObjAccessCube.ContainsKey(entry.Key))
                {
                    objCubeXMLROL = new SchemaRoleSchemaGrantCubeGrant();
                    objCubeXMLROL.cube = entry.Key;
                    objCubeXMLROL.access = SchemaRoleSchemaGrantCubeGrantAccess.none;
                    listCubeXMLROL.Add(objCubeXMLROL);
                    dictObjAccessCube.Add(entry.Key, new Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>());
                    countCheck++;
                }

            }
            //Виртуальные кубы
            foreach (KeyValuePair<String, Dictionary<String, String>> entry in dictDimensionByVirtualCubeXML)
            {

                SchemaRoleSchemaGrantCubeGrant objCubeXMLROL = null;
                if (!dictObjAccessCube.ContainsKey(entry.Key))
                {
                    objCubeXMLROL = new SchemaRoleSchemaGrantCubeGrant();
                    objCubeXMLROL.cube = entry.Key;
                    objCubeXMLROL.access = SchemaRoleSchemaGrantCubeGrantAccess.none;
                    listCubeXMLROL.Add(objCubeXMLROL);
                    dictObjAccessCube.Add(entry.Key, new Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>());
                    countCheck++;
                }

            }

            schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();
            int aa = 0;
            //Проверяем кубы
            for (int ind = 0; ind < schemaRoleXMLROL.SchemaGrant[0].CubeGrant.Length; ind++)
            {

                countCheck += checkHierarchyXMLROL(objSchema, schemaRoleXMLROL.name, ref schemaRoleXMLROL.SchemaGrant[0].CubeGrant[ind]);
                countCheck += checkDimensionXMLROL(objSchema, schemaRoleXMLROL.name, ref schemaRoleXMLROL.SchemaGrant[0].CubeGrant[ind]);
                aa++;
            }

            schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();

            return countCheck;
        }

        private int checkDimensionXMLROL(Schema objSchema, string roleName, ref SchemaRoleSchemaGrantCubeGrant cubeXMLROL)
        {
            int countCheck = 0;
            
            List<SchemaRoleSchemaGrantCubeGrantDimensionGrant> listDimensionXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrantDimensionGrant>();
            List<SchemaRoleSchemaGrantCubeGrantDimensionGrant> listDimensionXMLROL = null;
            Dictionary<String, String> dictHierarchyXMLROL = new Dictionary<String, String>();

            try
            {
                //Заполняем словарь иерархий с правами доступа
                
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemDimension in cubeXMLROL.HierarchyGrant.ToList())
                {
                    if(dictHierarchyXMLROL.ContainsKey(itemDimension.hierarchy)) continue;

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLres = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                    foreach (var resultHierarchy in cubeXMLROL.HierarchyGrant.ToList().Where(s => s.hierarchy == itemDimension.hierarchy))
                    {
                        listHierarchyXMLres.Add(resultHierarchy);
                    }

                    if(listHierarchyXMLres.Count == 1)
                    {
                        dictHierarchyXMLROL.Add(itemDimension.hierarchy, itemDimension.access.ToString());
                    } else if(listHierarchyXMLres.Count > 1) { 
                        int cntMembers = 0;
                        SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess hAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;

                        foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant reslst in listHierarchyXMLres)
                        { 
                            if(reslst.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom){ 
                                cntMembers = reslst.MemberGrant.Count();
                            } else { 
                                hAccess = reslst.access;
                            }
                        }
                        //если мемберов больше 0, то доступ = all, иначе доступ = hAccess
                        if(cntMembers>0)
                        { 
                            dictHierarchyXMLROL.Add(itemDimension.hierarchy, SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all.ToString());

                        }
                        else
                        {
                            dictHierarchyXMLROL.Add(itemDimension.hierarchy, hAccess.ToString());
                        }
                    
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkDimensionXMLROL, 1");
            }
       

            //Удаляем лишнее
            try
            {

                if (cubeXMLROL.DimensionGrant != null)
                {
                    listDimensionXMLROL = cubeXMLROL.DimensionGrant.ToList();
                    foreach (SchemaRoleSchemaGrantCubeGrantDimensionGrant itemDimension in listDimensionXMLROL)
                    {
                        if (!dictHierarchyXMLROL.ContainsKey(itemDimension.dimension))
                        { // по любому в dictHierarchyXMLcb иерархия обязана быть!
                            listDimensionXMLROLforDelete.Add(itemDimension);
                        }

                    }

                    foreach (SchemaRoleSchemaGrantCubeGrantDimensionGrant itemDimensionDel in listDimensionXMLROLforDelete)
                    {
                        listDimensionXMLROL.Remove(itemDimensionDel);
                        countCheck++;
                    }

                } 
                else 
                { 
                    listDimensionXMLROL = new List<SchemaRoleSchemaGrantCubeGrantDimensionGrant>();
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkDimensionXMLROL, 2");
            }

            //Добавляем недостающее
            try
            {

                foreach (KeyValuePair<String, String> entry in dictHierarchyXMLROL)
                {

                    //SchemaRoleSchemaGrantCubeGrantDimensionGrant objDimensionXMLROL = null;

                    bool bResOk = false;
                    foreach (SchemaRoleSchemaGrantCubeGrantDimensionGrant itemDimension in listDimensionXMLROL.Where(s => s.dimension == entry.Key))
                    {
                        bResOk = true;

                        if(!itemDimension.access.ToString().Equals(entry.Value) ){ 
                            
                            if(entry.Value.Equals("all"))
                            { 
                                itemDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.all;
                            } 
                            else
                            {
                                itemDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.none;
                            }
                            countCheck++;
                        }

                        break;
                    }

                    if (!bResOk)
                    {
                        SchemaRoleSchemaGrantCubeGrantDimensionGrant newDimension = new SchemaRoleSchemaGrantCubeGrantDimensionGrant();
                        newDimension.dimension = entry.Key;
                        if(entry.Value.Equals("all"))
                        { 
                            newDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.all;
                        } 
                        else
                        {
                            newDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.none;
                        }
                        
                        listDimensionXMLROL.Add(newDimension);
                        countCheck++;
                    } 

                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkDimensionXMLROL, 3");
            }
            cubeXMLROL.DimensionGrant = listDimensionXMLROL.ToArray();
            listDimensionXMLROL.Clear();



            return countCheck;
        }

        // Проверка целостности дименшенов в кубе
        private int checkHierarchyXMLROL(Schema objSchema, string roleName, ref SchemaRoleSchemaGrantCubeGrant cubeXMLROL)
        {

            int countCheck = 0;
            Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> dictObjAccessCubeXMLROL = dictObjAccessByRoleXMLROL[roleName];
            Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant> dictObjAccessHierarchyXMLROL = dictObjAccessCubeXMLROL[cubeXMLROL.cube];
            Dictionary<String, String> dictHierarchyXMLcb = null;

            try
            {

                if (dictDimensionByCubeXML.ContainsKey(cubeXMLROL.cube))
                {
                    dictHierarchyXMLcb = dictDimensionByCubeXML[cubeXMLROL.cube];
                }
                else if (dictDimensionByVirtualCubeXML.ContainsKey(cubeXMLROL.cube))
                {
                    dictHierarchyXMLcb = dictDimensionByVirtualCubeXML[cubeXMLROL.cube];
                }
                else
                {
                    dictHierarchyXMLcb = new Dictionary<String, String>();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkHierarchyXMLROL, 1");
            }

            Dictionary<String, Dictionary<String, bool>> dictHierarchyBase = null;
            Dictionary<String, bool> dictMemberBase = null;
            if (dictMemberByHierarhyByRoleXMLROL.ContainsKey(roleName))
            {
                dictHierarchyBase = dictMemberByHierarhyByRoleXMLROL[roleName];
            }

            //Проверка дименшенов - удаляем лишнее
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLROL = null;
            Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);

            try
            {

                if (cubeXMLROL.HierarchyGrant != null)
                {
                    listHierarchyXMLROL = cubeXMLROL.HierarchyGrant.ToList();
                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchy in listHierarchyXMLROL)
                    {

                        if (!dictHierarchyXMLcb.ContainsKey(itemHierarchy.hierarchy))
                        { // по любому в dictHierarchyXMLcb иерархия обязана быть!
                            listHierarchyXMLROLforDelete.Add(itemHierarchy);
                        }
                        else
                        {
                            if (dctDimensionWithAnnotation.ContainsKey(itemHierarchy.hierarchy))
                            {
                                //Проверяем, сколько таких (список одноименных дименшенов/иерархий)
                                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXML = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                                foreach (var resultHierarchy in listHierarchyXMLROL.Where(s => s.hierarchy == itemHierarchy.hierarchy))
                                {
                                    listHierarchyXML.Add(resultHierarchy);
                                }
                                //Если один и none, значит забанен и дальше не проверяем
                                if (listHierarchyXML.Count == 1 && listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none)
                                {
                                    continue;
                                }

                                List<string> etalonHierarhyList = generateHierarchyKeyListByDictMemberHierarhy(roleName, itemHierarchy.hierarchy);
                                String hierarhyKey = itemHierarchy.hierarchy + "##" + itemHierarchy.access.ToString();

                                bool bResOk = false;
                                foreach (var result in etalonHierarhyList.Where(s => s == hierarhyKey))
                                {
                                    bResOk = true;
                                    break;
                                }
                                //Ничего не нашли
                                if (!bResOk)
                                {
                                    listHierarchyXMLROLforDelete.Add(itemHierarchy);
                                }
                            }
                            else
                            {
                                //Если у дименшена аннотации нет, но есть мемберы или тип custom, то удаляем
                                if (itemHierarchy.MemberGrant != null || itemHierarchy.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                                {
                                    listHierarchyXMLROLforDelete.Add(itemHierarchy);
                                }
                            }
                        }
                    }

                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchyDel in listHierarchyXMLROLforDelete)
                    {
                        listHierarchyXMLROL.Remove(itemHierarchyDel);
                        countCheck++;
                    }

                }
                else
                {
                    listHierarchyXMLROL = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                }

                //Проверка дименшенов - создаем недостающие
                foreach (KeyValuePair<String, String> entry in dictHierarchyXMLcb)
                {

                    SchemaRoleSchemaGrantCubeGrantHierarchyGrant objHierarchyXMLROL = null;

                    if (dctDimensionWithAnnotation.ContainsKey(entry.Key))
                    {

                        //Проверяем, сколько таких (список одноименных дименшенов/иерархий)
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXML = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                        foreach (var resultHierarchy in listHierarchyXMLROL.Where(s => s.hierarchy == entry.Key))
                        {
                            listHierarchyXML.Add(resultHierarchy);
                        }
                        //Если один и none, значит забанен и дальше не проверяем
                        if (listHierarchyXML.Count == 1 && listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none)
                        {
                            continue;
                        }

                        List<string> etalonHierarhyList = generateHierarchyKeyListByDictMemberHierarhy(roleName, entry.Key);
                        foreach (string et in etalonHierarhyList)
                        {
                            string[] itemEt = et.Split("##");
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess etAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                            if (itemEt[1] == "all")
                            {
                                etAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all;
                            }
                            else if (itemEt[1] == "custom")
                            {
                                etAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom;
                            }

                            bool bResOk = false;
                            //Ищем объект в listHierarchyXMLROL с такими же именем и доступом, как в эталоне
                            foreach (var result in listHierarchyXMLROL.Where(s => s.hierarchy == itemEt[0] && s.access == etAccess))
                            {
                                bResOk = true;
                                break;
                            }
                            // если не нашли, то добавляем
                            if (!bResOk)
                            {
                                SchemaRoleSchemaGrantCubeGrantHierarchyGrant newHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                                newHierarchy.hierarchy = itemEt[0];
                                newHierarchy.access = etAccess;
                                newHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                                listHierarchyXMLROL.Add(newHierarchy);
                                countCheck++;
                            }
                        }

                    }
                    else
                    {

                        bool bResOk = false;
                        foreach (var result in listHierarchyXMLROL.Where(s => s.hierarchy == entry.Key))
                        {
                            bResOk = true;
                            break;
                        }

                        if (!bResOk)
                        {
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrant newHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                            newHierarchy.hierarchy = entry.Key;
                            newHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                            newHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                            listHierarchyXMLROL.Add(newHierarchy);
                            countCheck++;
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkHierarchyXMLROL, 2");
            }

            cubeXMLROL.HierarchyGrant = listHierarchyXMLROL.ToArray();
            listHierarchyXMLROL.Clear();

            try
            {

                for (int ind = 0; ind < cubeXMLROL.HierarchyGrant.Length; ind++)
                {

                    //Проверяем мемберы внутри иерархии (только те, у которых есть аннотации)
                    if (dctDimensionWithAnnotation.ContainsKey(cubeXMLROL.HierarchyGrant[ind].hierarchy))
                    {

                        //Проверяем только custom!!!
                        if (cubeXMLROL.HierarchyGrant[ind].access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                        {

                            countCheck += checkMembers(roleName, cubeXMLROL.HierarchyGrant[ind].hierarchy, ref cubeXMLROL.HierarchyGrant[ind]);
                        }
                        else
                        {

                            //Если не тип custom, то мемберов быть не должно!
                            if (cubeXMLROL.HierarchyGrant[ind].MemberGrant != null)
                            {
                                countCheck += cubeXMLROL.HierarchyGrant[ind].MemberGrant.Length;
                                cubeXMLROL.HierarchyGrant[ind].MemberGrant = null;
                            }
                        }
                    }

                    listHierarchyXMLROL.Add(cubeXMLROL.HierarchyGrant[ind]);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkHierarchyXMLROL, 3");
            }

            //Убираем Hierarchy с access=custom в конец списка
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLROLorder = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
            int checkOrder = 0; // режим отслеживания изменений
            foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant item in listHierarchyXMLROL)
            {
                if (item.access != SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                {
                    listHierarchyXMLROLorder.Add(item);
                    if (checkOrder == 1) checkOrder = 2;
                }
                else
                {
                    if (checkOrder == 0) checkOrder = 1;
                }
            }

            if (checkOrder == 2)
            { // Режим 2 - были изменения
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant item in listHierarchyXMLROL)
                {
                    if (item.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                    {
                        listHierarchyXMLROLorder.Add(item);
                    }
                }

                cubeXMLROL.HierarchyGrant = listHierarchyXMLROLorder.ToArray();
                countCheck++;
            }

            return countCheck;
        }

        //Получить мапку дименшенов с аннотациями
        public Dictionary<String, String> getDictionaryDimensionWithAnnotation(Schema objSchema)
        {

            if (dictDimensionWithAnnotation != null) return dictDimensionWithAnnotation;

            dictDimensionWithAnnotation = new Dictionary<String, String>();

            foreach (SharedDimension item in objSchema.Dimension.ToList())
            {

                if (item.Annotations != null && item.Annotations.Length > 0)
                {
                    dictDimensionWithAnnotation.Add(item.name, item.name);
                }
            }
            return dictDimensionWithAnnotation;
        }

        public int checkMembers(string roleName, String hierarchyName, ref SchemaRoleSchemaGrantCubeGrantHierarchyGrant objHierarchyXMLROL)
        {

            int countCheck = 0;
            //Список мемберов из XML
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listMemberXMLROL = null;

            if (objHierarchyXMLROL.MemberGrant != null)
            {
                listMemberXMLROL = objHierarchyXMLROL.MemberGrant.ToList();
            }
            else
            {
                listMemberXMLROL = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
            }

            //Список мемберов из основной мапки
            Dictionary<String, bool> dictMemberXMLROL = new Dictionary<string, bool>();
            Dictionary<String, bool> dictMemberBase = null;
            Dictionary<String, Dictionary<String, bool>> dictHierarchyBase = null;

            if (dictMemberByHierarhyByRoleXMLROL.ContainsKey(roleName))
            {
                dictHierarchyBase = dictMemberByHierarhyByRoleXMLROL[roleName];
            }

            if (dictHierarchyBase != null)
            {
                if (dictHierarchyBase.ContainsKey(hierarchyName))
                {
                    dictMemberBase = dictHierarchyBase[hierarchyName];
                }
                else
                {
                    dictMemberBase = new Dictionary<String, bool>();
                }
            }
            else
            {
                dictMemberBase = new Dictionary<String, bool>();
            }

            //Проверка мемберов - удаляем лишнее
            if (listMemberXMLROL.Count > 0)
            {

                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listMemberXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                //Готовим список для удаления
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMemberXMLROL in listMemberXMLROL)
                {
                    if (!dictMemberBase.ContainsKey(itemMemberXMLROL.member))
                    {
                        listMemberXMLROLforDelete.Add(itemMemberXMLROL);
                    }
                }
                //Удаляем
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMemberXMLROL in listMemberXMLROLforDelete)
                {
                    listMemberXMLROL.Remove(itemMemberXMLROL);
                    countCheck++;
                }

                objHierarchyXMLROL.MemberGrant = listMemberXMLROL.ToArray();
                listMemberXMLROL = objHierarchyXMLROL.MemberGrant.ToList();

                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMemberXMLROL in listMemberXMLROL)
                {
                    if (itemMemberXMLROL.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all)
                    {
                        dictMemberXMLROL.Add(itemMemberXMLROL.member, true);
                    }
                    else
                    {
                        dictMemberXMLROL.Add(itemMemberXMLROL.member, false);
                    }
                }

            }


            //Добавляем
            foreach (KeyValuePair<String, bool> entry in dictMemberBase)
            {

                SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = null;

                if (!dictMemberXMLROL.ContainsKey(entry.Key))
                {

                    objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant();
                    objMember.member = entry.Key;

                    if (entry.Value == true)
                    {
                        objMember.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all;
                    }
                    else
                    {
                        objMember.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none;
                    }

                    listMemberXMLROL.Add((SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant)objMember);
                    dictMemberXMLROL.Add(objMember.member, entry.Value);
                    countCheck++;
                }
            }

            objHierarchyXMLROL.MemberGrant = listMemberXMLROL.ToArray();

            return countCheck;
        }

        // Проверка целостности ролей
        public int checkRoleXML(bool bSave, ref Schema objSchema)
        {

            int countCheck = 0;
            List<SchemaRole> roleXmlForDelete = new List<SchemaRole>();

            //Список ролей из БД
            List<Role> listRole = getRoleList();
            Dictionary<String, Role> mapRoleBD = getMapRoleBD(listRole);

            //Список ролей из XML
            Dictionary<String, SchemaRole> mapRoleXML = getMapSchemaRole(objSchema);

            fillMemberDictionary(objSchema, bSave); //Заполняем основную мапку мемберов dictMemberByHierarhyByRoleXMLROL
            //Заполняем мапку реальными объектами доступа которые есть в схеме в секции Role
            countCheck += fillDictObjAccessByRoleXMLROL(objSchema);

            //Заполняем мапки кубов и виртуальных кубов, если они еще не заполнены
            filldictDimensionByCubeXML(objSchema);
            filldictDimensionByVirtualCubeXML(objSchema);

            //Удаляем лишние роли
            List<SchemaRole> lstShemRole = null;
            if (objSchema.Role == null)
            {
                lstShemRole = new List<SchemaRole>();
            }
            else
            {
                lstShemRole = objSchema.Role.ToList();
            }

            foreach (SchemaRole itemShemRole in lstShemRole)
            {
                if (!mapRoleBD.ContainsKey(itemShemRole.name))
                {
                    roleXmlForDelete.Add(itemShemRole);
                };
            }

            foreach (SchemaRole itemDel in roleXmlForDelete)
            {
                lstShemRole.Remove(itemDel);
                dictObjAccessByRoleXMLROL.Remove(itemDel.name);
                mapRoleXML.Remove(itemDel.name);
                countCheck++;
            }

            //Добавляем отсутствующие роли и контроль объектов доступа
            foreach (Role itemRole in listRole)
            {

                if (!dictObjAccessByRoleXMLROL.ContainsKey(itemRole.Name))
                {
                    //Создаем роль и объекты доступа заново
                    SchemaRole newSchemaRole = createNewSchemaRole(itemRole.Name);
                    lstShemRole.Add(newSchemaRole);
                    mapRoleXML.Add(itemRole.Name, newSchemaRole);
                    dictObjAccessByRoleXMLROL.Add(itemRole.Name, new Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>());  // ###
                    countCheck++;
                    countCheck += checkCubeXMLROL(objSchema, ref newSchemaRole);

                }
                else
                {
                    //Проверяем те, которые уже были
                    SchemaRole checkSchemaRole = mapRoleXML[itemRole.Name];
                    countCheck += checkCubeXMLROL(objSchema, ref checkSchemaRole);
                }

            }
            objSchema.Role = lstShemRole.ToArray();
            return countCheck;
        }

        private List<string> generateHierarchyKeyListByDictMemberHierarhy(String role, String hierarchy)
        {

            List<string> listOut = new List<string>();

            if (!dictMemberByHierarhyByRoleXMLROL.ContainsKey(role))
            {
                listOut.Add(hierarchy + "##all");
                return listOut;
            }

            Dictionary<String, Dictionary<String, bool>> dictHierarchy = dictMemberByHierarhyByRoleXMLROL[role];
            if (!dictHierarchy.ContainsKey(hierarchy))
            { // если нет ни одной записи для данной иерархии, что-то пошло не так. Все запрещаем.
                listOut.Add(hierarchy + "##none");
                return listOut;
            }

            Dictionary<String, bool> dictMember = dictHierarchy[hierarchy];
            foreach (KeyValuePair<String, bool> entry in dictMember)
            {

                if (entry.Value == true)
                {
                    listOut.Add(hierarchy + "##none");
                }
                else
                {
                    listOut.Add(hierarchy + "##all");
                }

                if (entry.Key != "noname")
                {
                    listOut.Add(hierarchy + "##custom");
                }
                return listOut;
            }
            return listOut;
        }

        public void fillMemberDictionary(Schema objSchema, bool withoutConditions)
        {
            string posError = "  && position: fillMemberDictionary, 0";

            //withoutConditions - выполнение в любом случае, даже если dictMemberByHierarhyByRoleXMLROL уже существует
            if (withoutConditions == false)
            {
                //Если основная мапка мемберов существует, считаем, что заполнять нечего и выходим.
                if (dictMemberByHierarhyByRoleXMLROL != null) return;
                dictMemberByHierarhyByRoleXMLROL = HttpContext.Session.GetObjectFromJson<Dictionary<String, Dictionary<String, Dictionary<String, bool>>>>("dictMemberByHierarhyByRoleXMLROLsession");
                if (dictMemberByHierarhyByRoleXMLROL != null) return;
            }

            try
            {
                //Удаление несуществующих ролей из stationroles
                string strsql = "delete from stationroles sd " +
                                "where  sd.id in ( " +
                                "    select str.id  " +
                                "    from stationroles str  " +
                                "    left outer join roles r on r.id = str.roleid  " +
                                "    where r.id is null " +
                                ")";

                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandText = strsql;
                    sql.CommandType = CommandType.Text;
                    sql.ExecuteNonQuery();
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
                posError = "  && position: fillMemberDictionary, 1";
                // Для каждой роли формируем массив "запрещенных" станций, т.е. станций, которые отсутствуют в таблице stationroles для данной роли
                // и массив "разрешенных" станций
                // в виде 233,278,317,324...
                string strsql2 = "select sg.roleid, sg.name as rolename, ss.stationlistban, sa.stationlistallowed " +
                                   " from ( select t.roleid, rl.name  from es.stationroles t " +
                                   "         inner join roles rl on rl.id = t.roleid " +
                                   "         group by t.roleid, rl.name " +
                                   "         ) sg, " +
                                   " lateral ( " +
                                   "     SELECT array_to_string(ARRAY( " +
                                   "         select st.id  " +
                                   "         from es.stations st " +
                                   "         left outer join (select * from es.stationroles where roleid = sg.roleid) str on str.stationid = st.id " +
                                   "         where str.stationid  is null " +
                                   "     ), ',') as stationlistban " +
                                   " ) ss, " +
                                   " lateral ( " +
                                   "     SELECT array_to_string(ARRAY( " +
                                   "         select st.id  " +
                                   "         from es.stations st " +
                                   "         inner join (select * from es.stationroles where roleid = sg.roleid) str on str.stationid = st.id " +
                                   "     ), ',') as stationlistallowed	 " +
                                   ") sa ";


                List<StationRoleOlapDTO> listStationRoleOlapDTO = new List<StationRoleOlapDTO>();
                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandText = strsql2;
                    sql.CommandType = CommandType.Text;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    while (reader.Read())
                    {
                        listStationRoleOlapDTO.Add(new StationRoleOlapDTO
                        {

                            RoleId = (long)reader["roleid"],
                            RoleName = (string)reader["rolename"],
                            StationListBan = (string)reader["stationlistban"],
                            StationListAllowed = (string)reader["stationlistallowed"]
                        });

                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }


                //Мапка Роль - Иерархия - Мембер
                dictMemberByHierarhyByRoleXMLROL = new Dictionary<String, Dictionary<String, Dictionary<String, bool>>>();
                //Список Мемберов
                int cnt = 0;
                foreach (StationRoleOlapDTO itemRole in listStationRoleOlapDTO)
                {
                    //Mапка Иерархия - общее число мемберо (станций)
                    Dictionary<String, long> dictToatalCountMember = new Dictionary<String, long>();
                    //Mапка Иерархия - общее число забаненых мемберов (станций)
                    Dictionary<String, long> dictBanCountMember = new Dictionary<String, long>();
                    StringBuilder sbSqlAnnotation = new StringBuilder("");
                    StringBuilder sbSqlAnnotationCountBan = new StringBuilder("");
                    StringBuilder sbSqlAnnotationCountTotal = new StringBuilder("");
                    //Заполняем мапку Иерархия - общее число мемберов (станций)
                    foreach (SharedDimension item in objSchema.Dimension.ToList())
                    {

                        if (item.Annotations != null && item.Annotations.Length > 0)
                        {
                            AnnotationsBase arrAnnTmp = null;
                            AnnotationsBase annotationsBase = item.Annotations[0];
                            AnnotationBase annotationBase = annotationsBase.Annotation[0];
                            AnnotationsAnnotation itemAnn = annotationBase.Annotation[0];

                            // sql общего числа станций
                            if (sbSqlAnnotationCountTotal.Length > 1)
                            {
                                sbSqlAnnotationCountTotal.Append(" union all ");
                            }
                            String sqlAnnTotal = "select '" + item.name + "' as dimensionname, count(*) as counttotal from ( " + itemAnn.Text[0] + " ) d ";
                            sqlAnnTotal = sqlAnnTotal.Replace("?_operatoin_in_?", "not in");
                            sqlAnnTotal = sqlAnnTotal.Replace("?stationid?", "-100");
                            sbSqlAnnotationCountTotal.Append(sqlAnnTotal);
                        }
                    }

                    if (sbSqlAnnotationCountTotal.ToString().Length > 2)
                    {
                        using (NpgsqlCommand sql = new NpgsqlCommand())
                        {
                            sql.Connection = _db.GetNpgsqlConnection();
                            sql.CommandText = sbSqlAnnotationCountTotal.ToString();
                            sql.CommandType = CommandType.Text;
                            posError = "  && position: fillMemberDictionary, 2, itemRole = '" + itemRole.RoleName + "'";
                            NpgsqlDataReader reader = sql.ExecuteReader();
                            while (reader.Read())
                            {
                                dictToatalCountMember.Add((string)reader["dimensionname"], (long)reader["counttotal"]);
                            }

                            sql.Connection.Close();
                            sql.Connection.Dispose();
                        }
                    }

                    //Заполняем мапку Иерархия - общее число забаненых мемберов (станций)
                    foreach (SharedDimension item in objSchema.Dimension.ToList())
                    {

                        if (item.Annotations != null && item.Annotations.Length > 0)
                        {
                            AnnotationsBase arrAnnTmp = null;
                            AnnotationsBase annotationsBase = item.Annotations[0];
                            AnnotationBase annotationBase = annotationsBase.Annotation[0];
                            AnnotationsAnnotation itemAnn = annotationBase.Annotation[0];
                            // sql числа запрещенных станций
                            if (sbSqlAnnotationCountBan.Length > 1)
                            {
                                sbSqlAnnotationCountBan.Append(" union all ");
                            }
                            String sqlAnnBan = "select '" + item.name + "' as dimensionname, count(*) as countban from ( " + itemAnn.Text[0] + " ) d ";
                            sqlAnnBan = sqlAnnBan.Replace("?stationid?", itemRole.StationListBan);
                            sqlAnnBan = sqlAnnBan.Replace("?_operatoin_in_?", "in");
                            sbSqlAnnotationCountBan.Append(sqlAnnBan);
                        }
                    }

                    cnt++;
                    if(cnt == 23){ 
                        cnt = cnt;
                    }
                    //ИБ администраторы EMAS
                    if (sbSqlAnnotationCountBan.ToString().Length > 2)
                    {
                        using (NpgsqlCommand sql = new NpgsqlCommand())
                        {
                            sql.Connection = _db.GetNpgsqlConnection();
                            sql.CommandText = sbSqlAnnotationCountBan.ToString();
                            sql.CommandType = CommandType.Text;
                            posError = "  && position: fillMemberDictionary, 3, itemRole = '" + itemRole.RoleName + "'";
                            NpgsqlDataReader reader = sql.ExecuteReader();
                            while (reader.Read())
                            {
                                dictBanCountMember.Add((string)reader["dimensionname"], (long)reader["countban"]);

                            }

                            sql.Connection.Close();
                            sql.Connection.Dispose();
                        }
                    }

                    //Идем по всем кубам с аннотациями и получаем SQL-запросы для мемберов
                    foreach (SharedDimension item in objSchema.Dimension.ToList())
                    {
                        posError = "  && position: fillMemberDictionary, 4, itemRole = '" + itemRole.RoleName + "'";

                        if (item.Annotations != null && item.Annotations.Length > 0)
                        {
                            AnnotationsBase arrAnnTmp = null;
                            AnnotationsBase annotationsBase = item.Annotations[0];
                            AnnotationBase annotationBase = annotationsBase.Annotation[0];
                            AnnotationsAnnotation itemAnn = annotationBase.Annotation[0];

                            long countTotal = 0;
                            long countBan = 0;
                            if (dictToatalCountMember.ContainsKey(item.name))
                            {
                                countTotal = dictToatalCountMember[item.name];
                            }
                            if (dictBanCountMember.ContainsKey(item.name))
                            {
                                countBan = dictBanCountMember[item.name];
                            }

                            if (itemAnn.name == "sqlAnnotation")
                            {

                                if (sbSqlAnnotation.Length > 1)
                                {
                                    sbSqlAnnotation.Append(" union all ");
                                }

                                String sqlAnn = itemAnn.Text[0];

                                if (countTotal / 2 <= countBan)
                                {
                                    sqlAnn = sqlAnn.Replace("?stationid?", itemRole.StationListAllowed); // ban passive
                                }
                                else
                                {
                                    sqlAnn = sqlAnn.Replace("?stationid?", itemRole.StationListBan); // ban active
                                }
                                sqlAnn = sqlAnn.Replace("?_operatoin_in_?", "in");

                                sbSqlAnnotation.Append(sqlAnn);

                            }

                        }

                    }

                    if (sbSqlAnnotation.Length < 1) continue;

                    List<DimensionMemberDTO> listDimensionMemberDTO = new List<DimensionMemberDTO>();
                    using (NpgsqlCommand sql = new NpgsqlCommand())
                    {
                        sql.Connection = _db.GetNpgsqlConnection();
                        sql.CommandText = sbSqlAnnotation.ToString();
                        sql.CommandType = CommandType.Text;
                        posError = "  && position: fillMemberDictionary, 5, itemRole = '" + itemRole.RoleName + "'";
                        NpgsqlDataReader reader = sql.ExecuteReader();

                        while (reader.Read())
                        {
                            string hierarhyKey = (string)reader["dimensionname"];
                            string strDimensionmember = (string)reader["dimensionmember"];
                            long countTotal = 0;
                            long countBan = 0;
                            bool bAccessInMember = false;
                            if (itemRole.StationListAllowed.Length == 0)
                            { //Для роли ни один флаг не выставлен => все разрешено
                                bAccessInMember = false;
                                strDimensionmember = "noname";
                            }
                            else
                            {
                                if (dictToatalCountMember.ContainsKey(hierarhyKey))
                                {
                                    countTotal = dictToatalCountMember[hierarhyKey];
                                }
                                if (dictBanCountMember.ContainsKey(hierarhyKey))
                                {
                                    countBan = dictBanCountMember[hierarhyKey];
                                }
                                if (countTotal / 2 <= countBan)
                                {
                                    bAccessInMember = true;
                                }

                                if (countTotal == countBan)
                                { // все забанено
                                    bAccessInMember = true;
                                    strDimensionmember = "noname";
                                }

                            }

                            listDimensionMemberDTO.Add(new DimensionMemberDTO
                            {
                                DimensionName = hierarhyKey,
                                DimensionMember = strDimensionmember,
                                AccessInMember = bAccessInMember
                            });
                        }
                        sql.Connection.Close();
                        sql.Connection.Dispose();
                    }

                    posError = "  && position: fillMemberDictionary, 6";
                    if (listDimensionMemberDTO.Count < 1) continue;

                    Dictionary<String, Dictionary<String, bool>> dictMemberByHierarhy = null;
                    if (dictMemberByHierarhyByRoleXMLROL.ContainsKey(itemRole.RoleName))
                    {
                        dictMemberByHierarhy = dictMemberByHierarhyByRoleXMLROL[itemRole.RoleName];
                    }
                    else
                    {
                        dictMemberByHierarhy = new Dictionary<String, Dictionary<String, bool>>();
                        dictMemberByHierarhyByRoleXMLROL.Add(itemRole.RoleName, dictMemberByHierarhy);
                    }


                    foreach (DimensionMemberDTO itemDimensionMemberDTO in listDimensionMemberDTO)
                    {

                        //Проверка на вложенные скобки
                        var matchesSquareBracket = Regex.Matches(itemDimensionMemberDTO.DimensionMember, @"[[\]]+").Cast<Match>().Select(i => i.Value).ToArray();
                        string textoutSquareBracket = string.Join("", matchesSquareBracket);
                        var matchesSquareBracket2 = Regex.Matches(textoutSquareBracket, @"(.)\1+");
                        if (matchesSquareBracket2.Count > 0)
                        {
                            continue;
                        }

                        Dictionary<String, bool> dictMember = null;
                        if (dictMemberByHierarhy.ContainsKey(itemDimensionMemberDTO.DimensionName))
                        {
                            dictMember = dictMemberByHierarhy[itemDimensionMemberDTO.DimensionName];
                        }
                        else
                        {
                            dictMember = new Dictionary<String, bool>();
                            dictMemberByHierarhy.Add(itemDimensionMemberDTO.DimensionName, dictMember);
                        }

                        if (!dictMember.ContainsKey(itemDimensionMemberDTO.DimensionMember))
                        {
                            dictMember.Add(itemDimensionMemberDTO.DimensionMember, itemDimensionMemberDTO.AccessInMember);
                        }
                    }
                }
                posError = "  && position: fillMemberDictionary, 7";
                HttpContext.Session.SetObjectAsJson("dictMemberByHierarhyByRoleXMLROLsession", dictMemberByHierarhyByRoleXMLROL);

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return;
        }

        private void schemaSave(Schema objSchema)
        {

            string filePath = getShemaFilePath();

            try
            {
                XmlSerializer serializer = new XmlSerializer(objSchema.GetType());
                using (XmlTextWriter tw = new XmlTextWriter(filePath, new UTF8Encoding(false)))
                {
                    tw.Formatting = System.Xml.Formatting.Indented;
                    serializer.Serialize(tw, objSchema);
                }

            }
            catch (Exception e)
            {
                throw new Exception(e.Message + "  && Ошибка при сохранении файла с OLAP-схемой, schemaSave, 1 ");
            }

            /*
            //Перезагрузка сервера после сохранения
            string username = null;
            string password = null;
            string url = null; 

            try
            {
                username   = getUserName_OlapServerRestart();
            }
            catch(Exception e)
            {
                throw new Exception("Не удалось получить имя пользователя для перезагрузки OLAP-сервера из таблицы настроек: " + e.Message + "  && schemaSave, 2");  
            }

            try
            {
                password   = getPassword_OlapServerRestart();
            }
            catch(Exception e)
            {
                throw new Exception("Не удалось получить пароль для перезагрузки OLAP-сервера из таблицы настроек: " + e.Message + "  && schemaSave, 3");  
            }

            try
            {
                url = getUrl_OlapServerRestart();
            }
            catch(Exception e)
            {
                throw new Exception("Не удалось получить URL для перезагрузки OLAP-сервера из таблицы настроек: " + e.Message + "  && schemaSave, 4");  
            }
            
            string encoded = System.Convert.ToBase64String(Encoding.GetEncoding("ISO-8859-1")
                                           .GetBytes(username + ":" + password));
            
            using var client = new HttpClient();
            var msg = new HttpRequestMessage(HttpMethod.Get, url);
            msg.Headers.Add("Authorization", "Basic " + encoded);

            try
            {
                var res =  client.Send(msg);
                var status = res.StatusCode;
                if(res.StatusCode == System.Net.HttpStatusCode.OK)
                { 
                    return;
                } 
                else 
                { 
                    throw new Exception("Не удалось перезагрузить сервер с мондрианом"  + "  && schemaSave, 5");
                }
            } 
            catch(Exception e)
            { 
                throw new Exception("Не удалось перезагрузить сервер с мондрианом: " + e.Message  + "  && schemaSave, 6");    
            }
            */
        }
        public Schema savePermissionChangesForCubes()
        {
            String posError = "  && savePermissionChangesForCubes, 0";
            String storageKey = null;
            Schema objSchema = null;
            int countCheck = 0;
            try
            {
                objSchema = getShema();
                countCheck = checkRoleXML(false, ref objSchema);
                storageKey = "PermissionOLAPCubeDataXML_" + HttpContext.Session.GetObjectFromJson<string>("CubeOlapAccessGUID");

                posError = "  && savePermissionChangesForCubes, 1";
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }
                posError = "  && savePermissionChangesForCubes, 2";
                Dictionary<string, string> dictPermissionCubeData = PermissionDataStorage[storageKey];

                if (dictPermissionCubeData == null) return objSchema;
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);
                Dictionary<String, Dictionary<String, bool>> dictRoleChange = new Dictionary<String, Dictionary<String, bool>>();

                String cubeNameForSave = null;
                String roleKeyForSave = null;
                bool valueForSave = false;
                Dictionary<string, object> dictValeForSave = null;
                Dictionary<String, bool> distValue = null;

                posError = "  && savePermissionChangesForCubes, 3";
                //Разворачиваем в мапку по ролям
                foreach (KeyValuePair<string, string> entry in dictPermissionCubeData)
                {
                    cubeNameForSave = entry.Key;
                    dictValeForSave = JsonConvert.DeserializeObject<Dictionary<string, object>>(entry.Value);
                    foreach (KeyValuePair<string, object> item in dictValeForSave)
                    {
                        roleKeyForSave = item.Key;
                        valueForSave = (bool)item.Value;

                        if (!dictRoleChange.ContainsKey(roleKeyForSave))
                        {
                            distValue = new Dictionary<String, bool>();
                            dictRoleChange.Add(roleKeyForSave, distValue);
                        }
                        else
                        {
                            distValue = dictRoleChange[roleKeyForSave];
                        }
                        if (!distValue.ContainsKey(cubeNameForSave))
                        {
                            distValue.Add(cubeNameForSave, valueForSave);
                        }
                    }

                }

                posError = "  && savePermissionChangesForCubes, 4";
                //Мапка ключей и наименований ролей
                Dictionary<string, string> distKeyRole = new Dictionary<string, string>();
                List<Role> listRole = getRoleList();
                foreach (Role item in listRole)
                {
                    distKeyRole.Add(roleIdGenerate(item.Name), item.Name);
                }

                posError = "  && savePermissionChangesForCubes, 5";
                //Пробегаемся по мапке по ролям и модифицируем SchemaRole
                foreach (KeyValuePair<string, Dictionary<String, bool>> itemRoleChange in dictRoleChange)
                {

                    if (!distKeyRole.ContainsKey(itemRoleChange.Key)) { continue; }
                    string exchangeRole = distKeyRole[itemRoleChange.Key];

                    if (!mapRole.ContainsKey(exchangeRole)) { continue; }

                    distValue = itemRoleChange.Value;
                    SchemaRole objSchemaRole = mapRole[exchangeRole];
                    List<SchemaRoleSchemaGrantCubeGrant> lstCube = objSchemaRole.SchemaGrant[0].CubeGrant.ToList();

                    foreach (SchemaRoleSchemaGrantCubeGrant itemCubeGrant in lstCube)
                    {

                        if (!distValue.ContainsKey(itemCubeGrant.cube)) continue;
                        bool? val = distValue[itemCubeGrant.cube];
                        if (val == null) val = false;

                        if (val == true)
                        {
                            itemCubeGrant.access = SchemaRoleSchemaGrantCubeGrantAccess.all;
                        }
                        else
                        {
                            itemCubeGrant.access = SchemaRoleSchemaGrantCubeGrantAccess.none;
                        }

                    }
                    objSchemaRole.SchemaGrant[0].CubeGrant = lstCube.ToArray();
                }

                posError = "  && savePermissionChangesForCubes, 6";
                //Сохраняем
                schemaSave(objSchema);

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            finally
            {

                if (storageKey != null)
                {
                    PermissionDataStorage.Remove(storageKey);
                }
            }
            return objSchema;
        }

        public Schema savePermissionChangesForDimensions()
        {
            String posError = "  && position: savePermissionChangesForDimensions, 0";
            String storageKey = null;
            Schema objSchema = null;
            int countCheck = 0;
            try
            {
                objSchema = getShema();
                countCheck = checkRoleXML(false, ref objSchema);
                storageKey = "PermissionOLAPDimensionsDataXML_" + HttpContext.Session.GetObjectFromJson<string>("DimensionOlapAccessGUID");

                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }

                posError = "  && position: savePermissionChangesForDimensions, 1";
                Dictionary<string, string> dictPermissionDimensionData = PermissionDataStorage[storageKey];

                if (dictPermissionDimensionData == null)
                {
                    schemaSave(objSchema);
                    return objSchema;
                }
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);
                Dictionary<String, Dictionary<String, bool>> dictRoleChange = new Dictionary<String, Dictionary<String, bool>>();

                String dimensionNameForSave = null;
                String roleKeyForSave = null;
                bool valueForSave = false;
                Dictionary<string, object> dictValeForSave = null;
                Dictionary<String, bool> distValue = null;

                posError = "  && position: savePermissionChangesForDimensions, 2";
                //Разворачиваем в мапку по ролям dictRoleChange: [роль , [дименшн, значение]]
                foreach (KeyValuePair<string, string> entry in dictPermissionDimensionData)
                {
                    dimensionNameForSave = entry.Key;
                    dictValeForSave = JsonConvert.DeserializeObject<Dictionary<string, object>>(entry.Value);
                    foreach (KeyValuePair<string, object> item in dictValeForSave)
                    {
                        roleKeyForSave = item.Key;
                        valueForSave = (bool)item.Value;

                        if (!dictRoleChange.ContainsKey(roleKeyForSave))
                        {
                            distValue = new Dictionary<String, bool>();
                            dictRoleChange.Add(roleKeyForSave, distValue);
                        }
                        else
                        {
                            distValue = dictRoleChange[roleKeyForSave];
                        }
                        if (!distValue.ContainsKey(dimensionNameForSave))
                        {
                            distValue.Add(dimensionNameForSave, valueForSave);
                        }
                    }
                }

                posError = "  && position: savePermissionChangesForDimensions, 3";
                //Мапка ключей и наименований ролей
                Dictionary<string, string> distKeyRole = new Dictionary<string, string>();
                List<Role> listRole = getRoleList();
                foreach (Role item in listRole)
                {
                    distKeyRole.Add(roleIdGenerate(item.Name), item.Name);
                }

                Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);
                posError = "  && position: savePermissionChangesForDimensions, 4";
                //Пробегаемся по мапке по ролям и модифицируем SchemaRole
                foreach (KeyValuePair<string, Dictionary<String, bool>> itemRoleChange in dictRoleChange)
                {

                    if (!distKeyRole.ContainsKey(itemRoleChange.Key)) { continue; }
                    string exchangeRole = distKeyRole[itemRoleChange.Key];

                    if (!mapRole.ContainsKey(exchangeRole)) { continue; }

                    distValue = itemRoleChange.Value;
                    SchemaRole objSchemaRole = mapRole[exchangeRole];

                    if (objSchemaRole.SchemaGrant == null) continue;
                    if (objSchemaRole.SchemaGrant[0].CubeGrant == null) continue;

                    List<SchemaRoleSchemaGrantCubeGrant> lstCube = objSchemaRole.SchemaGrant[0].CubeGrant.ToList();

                    foreach (SchemaRoleSchemaGrantCubeGrant itemCubeGrant in lstCube)
                    { //Идем по кубам нужной роли

                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> lstHierarchy = itemCubeGrant.HierarchyGrant.ToList();
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> lstHierarchyForAdd = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> lstHierarchyForDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                        Dictionary<string, string> controlName = new Dictionary<string, string>(); //Иерархии, которые уже проверены

                        foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchy in lstHierarchy)
                        {

                            if (!distValue.ContainsKey(itemHierarchy.hierarchy)) continue;

                            if (controlName.ContainsKey(itemHierarchy.hierarchy)) continue;

                            controlName.Add(itemHierarchy.hierarchy, itemHierarchy.hierarchy);

                            //Нашли иерархию, которую нужно менять
                            //Новое значение:
                            bool val = distValue[itemHierarchy.hierarchy];

                            if (dctDimensionWithAnnotation.ContainsKey(itemHierarchy.hierarchy))
                            {

                                if (val == true)
                                {
                                    //Просто добавляем пустой custom
                                    SchemaRoleSchemaGrantCubeGrantHierarchyGrant hewHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                                    hewHierarchy.hierarchy = itemHierarchy.hierarchy;
                                    hewHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                                    hewHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom;
                                    lstHierarchyForAdd.Add(hewHierarchy);

                                }
                                else
                                {
                                    //Убираем custom, не custom ставим none
                                    int cnt = 0;
                                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant resultHierarchy in lstHierarchy.Where(s => s.hierarchy == itemHierarchy.hierarchy))
                                    {
                                        if (cnt == 0)
                                        {
                                            resultHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                                            resultHierarchy.MemberGrant = null;
                                        }
                                        else
                                        {
                                            lstHierarchyForDelete.Add(resultHierarchy);
                                        }
                                        cnt = 1;
                                    }
                                }

                            }
                            else
                            {

                                if (val == true)
                                {
                                    itemHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all;
                                }
                                else
                                {
                                    itemHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                                }
                            }

                        }

                        if (lstHierarchyForDelete.Count > 0)
                        {
                            lstHierarchy = lstHierarchy.Except(lstHierarchyForDelete).ToList();
                        }
                        if (lstHierarchyForAdd.Count > 0)
                        {
                            lstHierarchy = lstHierarchy.Concat(lstHierarchyForAdd).ToList();
                        }

                        itemCubeGrant.HierarchyGrant = lstHierarchy.ToArray();
                    }

                    objSchemaRole.SchemaGrant[0].CubeGrant = lstCube.ToArray();

                }
                posError = "  && position: savePermissionChangesForDimensions, 5";
                countCheck = checkRoleXML(true, ref objSchema);
                //Сохраняем
                schemaSave(objSchema);

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            finally
            {

                if (storageKey != null)
                {
                    PermissionDataStorage.Remove(storageKey);
                }
            }
            return objSchema;
        }

        public Schema savePermissionChangesForRoleStation(List<Role> listRole, List<Station> listStation)
        {
            String posError = "  && position: savePermissionChangesForRoleStation, 0";
            String storageKey = null;
            Schema objSchema = null;
            int countCheck = 0;
            try
            {
                objSchema = getShema();
                storageKey = "PermissionOLAPRoleStationDataXML_" + HttpContext.Session.GetObjectFromJson<string>("RoleStationOlapAccessGUID");

                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    countCheck = checkRoleXML(false, ref objSchema);
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }
                posError = "  && position: savePermissionChangesForRoleStation, 1";
                Dictionary<string, string> dictPermissionRoleStationData = PermissionDataStorage[storageKey];

                if (dictPermissionRoleStationData == null)
                {
                    countCheck = checkRoleXML(false, ref objSchema);
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }
                posError = "  && position: savePermissionChangesForRoleStation, 2";
                Dictionary<string, long> dictRole = new Dictionary<string, long>();

                foreach (Role item in listRole)
                {
                    dictRole.Add(roleIdGenerate(item.Name), item.Id);
                }
                long StationRoleId = _db.Stationroles.Max(p => p.Id);
                posError = "  && position: savePermissionChangesForRoleStation, 3";
                foreach (KeyValuePair<string, string> entry in dictPermissionRoleStationData)
                {
                    string stationName = entry.Key;
                    long stationId = 0;
                    Station fStation = listStation.FirstOrDefault(p => p.Name == stationName, null);
                    if (fStation == null) continue;
                    stationId = fStation.Id;

                    Dictionary<string, object> dictValeForSave = JsonConvert.DeserializeObject<Dictionary<string, object>>(entry.Value);
                    foreach (KeyValuePair<string, object> item in dictValeForSave)
                    {

                        string roleName = item.Key;
                        if (!dictRole.ContainsKey(roleName)) continue;
                        long roleId = dictRole[roleName];

                        bool valueForSave = (bool)item.Value;
                        Stationrole stationRoleEx = null;
                        try
                        {
                            stationRoleEx = _db.Stationroles.FirstOrDefault(p => p.Roleid == roleId && p.Stationid == stationId);
                        }
                        catch (Exception ex)
                        {
                            //Ничего не делаем, просто идем дальше.
                        }

                        if (valueForSave == false && stationRoleEx != null)
                        {
                            _db.Stationroles.Remove(stationRoleEx);
                        }
                        else if (valueForSave == true && stationRoleEx == null)
                        {
                            stationRoleEx = new Stationrole
                            {
                                Id = ++StationRoleId,
                                Roleid = roleId,
                                Stationid = stationId
                            };
                            _db.Stationroles.Add(stationRoleEx);
                        }

                    }
                }
                posError = "  && position: savePermissionChangesForRoleStation, 4";
                _db.SaveChanges();
                countCheck = checkRoleXML(true, ref objSchema);
                if (countCheck > 0)
                {
                    schemaSave(objSchema);
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            finally
            {

                if (storageKey != null)
                {
                    PermissionDataStorage.Remove(storageKey);
                }
            }
            return objSchema;
        }


        // Data for CubesPermission Table
        public DataTable GetCubeGridData()
        {
            String posError = "  && position: GetCubeGridData, 0";
            DataTable result = new DataTable();

            try
            {
                Schema objSchema = savePermissionChangesForCubes();
                Dictionary<string, DataRow> map = new Dictionary<string, DataRow>();
                long idIndex = 0;

                List<Role> listRole = getRoleList();
                result.Columns.Add("OlapName");

                foreach (Role item in listRole)
                {
                    result.Columns.Add(roleIdGenerate(item.Name), typeof(bool));
                }
                posError = "  && position: GetCubeGridData, 1";
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);
                List<SchemaRoleSchemaGrantCubeGrant> lstCube = objSchema.Role[0].SchemaGrant[0].CubeGrant.ToList();
                DataRow lineRow = null;
                posError = "  && position: GetCubeGridData, 2";
                foreach (SchemaRoleSchemaGrantCubeGrant itemCube in lstCube)
                {
                    if(dictCubeVisible.ContainsKey(itemCube.cube))
                    {
                        if(dictCubeVisible[itemCube.cube] == false) continue;
                    }
                    else continue;

                    lineRow = result.NewRow();
                    lineRow["OlapName"] = itemCube.cube;
                    Boolean val = new Boolean();
                    

                    foreach (Role item in listRole)
                    {
                        val = false;
                        if (mapRole.ContainsKey(item.Name))
                        {

                            SchemaRole roleXML = mapRole[item.Name];
                            if (roleXML == null) break;

                            if (roleXML.SchemaGrant[0].CubeGrant != null)
                            {
                                foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXML in roleXML.SchemaGrant[0].CubeGrant)
                                {
                                    if (itemCubeXML.cube.Equals(itemCube.cube))
                                    {

                                        if (itemCubeXML.access == SchemaRoleSchemaGrantCubeGrantAccess.all)
                                        {
                                            val = true;
                                        }
                                        else
                                        {
                                            val = false;
                                        }
                                    }

                                }
                            }
                            else
                            {
                                val = false;
                            }
                        }
                        lineRow[roleIdGenerate(item.Name)] = val;

                    }
                    result.Rows.Add(lineRow);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return result;
        }

        // Data for DimensionsPermission Table
        public DataTable GetDimensionGridData()
        {
            String posError = "  && position: GetDimensionGridData, 0";
            DataTable result = new DataTable();

            try
            {
                Schema objSchema = savePermissionChangesForDimensions();
                Dictionary<string, DataRow> map = new Dictionary<string, DataRow>();
                long idIndex = 0;
                Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);

                List<Role> listRole = getRoleList();
                result.Columns.Add("OlapName");

                foreach (Role item in listRole)
                {
                    result.Columns.Add(roleIdGenerate(item.Name), typeof(bool));
                }
                posError = "  && position: GetDimensionGridData, 1";
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);

                SharedDimension[] arrDimension = objSchema.Dimension;
                if (arrDimension == null) return result;
                List<SharedDimension> lstDimension = objSchema.Dimension.ToList();
                posError = "  && position: GetDimensionGridData, 2";
                DataRow lineRow = null;
                foreach (SharedDimension itemDimension in lstDimension)
                {
                    lineRow = result.NewRow();
                    lineRow["OlapName"] = itemDimension.name;
                    bool val = false;

                    foreach (Role item in listRole)
                    {
                        val = false;
                        if (mapRole.ContainsKey(item.Name))
                        {
                            bool flBreak = false;
                            SchemaRole roleXML = mapRole[item.Name];

                            if (roleXML.SchemaGrant == null) continue;
                            if (roleXML.SchemaGrant[0].CubeGrant == null) continue;

                            foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXMLROL in roleXML.SchemaGrant[0].CubeGrant)
                            {

                                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXML = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                                foreach (var resultHierarchy in itemCubeXMLROL.HierarchyGrant.ToList().Where(s => s.hierarchy == itemDimension.name))
                                {
                                    listHierarchyXML.Add(resultHierarchy);
                                }

                                if (listHierarchyXML.Count == 0) { continue; }

                                if (dctDimensionWithAnnotation.ContainsKey(itemDimension.name))
                                {

                                    //Если только один элемент и none, значит запрещен
                                    //В любом другом случае - разрешен
                                    if (listHierarchyXML.Count == 1 && listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none)
                                    {
                                        val = false;
                                    }
                                    else
                                    {
                                        val = true;
                                    }

                                }
                                else
                                {

                                    if (listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all)
                                    {
                                        val = true;
                                    }
                                    else
                                    {
                                        val = false;
                                    }
                                }
                                break;
                            }
                        }
                        lineRow[roleIdGenerate(item.Name)] = val;
                    }
                    result.Rows.Add(lineRow);
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            return result;
        }

        public DataTable GetRolestationGridData()
        {
            String posError = "  && position: GetRolestationGridData, 0";
            DataTable result = new DataTable();

            try
            {
                List<Role> listRole = getRoleList();
                List<Station> listStation = getStationList();
                Schema objSchema = savePermissionChangesForRoleStation(listRole, listStation);
                Dictionary<string, Dictionary<string, string>> objStationRole = getStationRoleDict();

                Dictionary<string, DataRow> map = new Dictionary<string, DataRow>();
                long idIndex = 0;
                result.Columns.Add("OlapName");

                foreach (Role item in listRole)
                {
                    result.Columns.Add(roleIdGenerate(item.Name), typeof(bool));
                }

                posError = "  && position: GetRolestationGridData, 1";
                DataRow lineRow = null;
                foreach (Station itemStation in listStation)
                {
                    lineRow = result.NewRow();
                    lineRow["OlapName"] = itemStation.Name;
                    Dictionary<string, string> dictRolesByStationRoles = null;
                    if (objStationRole.ContainsKey(itemStation.Name))
                    {
                        dictRolesByStationRoles = objStationRole[itemStation.Name];
                    }

                    foreach (Role item in listRole)
                    {
                        String roleId = roleIdGenerate(item.Name);
                        lineRow[roleId] = false;
                        if (dictRolesByStationRoles == null) continue;
                        if (!dictRolesByStationRoles.ContainsKey(item.Name)) continue;
                        lineRow[roleId] = true;
                    }
                    result.Rows.Add(lineRow);
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return result;

        }

        protected IEnumerable<Dictionary<string, object>> ToDataTableList(DataTable table)
        {
            string[] columns = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
            IEnumerable<Dictionary<string, object>> result = table.Rows.Cast<DataRow>()
                    .Select(dr => columns.ToDictionary(c => c, c => dr[c]));
            return result;
        }

        static object lockerCube = new object();

        public IActionResult saveOlapPermissionsCube(string key, string values)
        {
            bool lockWasTaken = false;
            Monitor.Enter(lockerCube, ref lockWasTaken);
            try
            {
                String storageKey = "PermissionOLAPCubeDataXML_" + HttpContext.Session.GetObjectFromJson<string>("CubeOlapAccessGUID");

                Dictionary<string, string> dictPermissionCubeData = null;
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    dictPermissionCubeData = new Dictionary<string, string>();
                    PermissionDataStorage.Add(storageKey, dictPermissionCubeData);
                }
                else
                {
                    dictPermissionCubeData = PermissionDataStorage[storageKey];
                }

                if (dictPermissionCubeData.ContainsKey(key))
                {
                    dictPermissionCubeData[key] = values;
                }
                else
                {
                    dictPermissionCubeData.Add(key, values);
                }

            }
            finally
            {
                if (lockWasTaken)
                {
                    Monitor.Exit(lockerCube);
                }
            }
            return Ok();

        }

        static object lockerDimension = new object();

        public IActionResult saveOlapPermissionsDimension(string key, string values)
        {
            bool lockWasTaken = false;
            Monitor.Enter(lockerDimension, ref lockWasTaken);
            try
            {
                String storageKey = "PermissionOLAPDimensionsDataXML_" + HttpContext.Session.GetObjectFromJson<string>("DimensionOlapAccessGUID");

                Dictionary<string, string> dictPermissionPermissionsData = null;
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    dictPermissionPermissionsData = new Dictionary<string, string>();
                    PermissionDataStorage.Add(storageKey, dictPermissionPermissionsData);
                }
                else
                {
                    dictPermissionPermissionsData = PermissionDataStorage[storageKey];
                }

                if (dictPermissionPermissionsData.ContainsKey(key))
                {
                    dictPermissionPermissionsData[key] = values;
                }
                else
                {
                    dictPermissionPermissionsData.Add(key, values);
                }

            }
            finally
            {
                if (lockWasTaken)
                {
                    Monitor.Exit(lockerDimension);
                }
            }
            return Ok();
        }


        static object lockerRoleStation = new object();

        public IActionResult saveOlapPermissionsRoleStation(string key, string values)
        {
            bool lockWasTaken = false;
            Monitor.Enter(lockerRoleStation, ref lockWasTaken);
            try
            {
                String storageKey = "PermissionOLAPRoleStationDataXML_" + HttpContext.Session.GetObjectFromJson<string>("RoleStationOlapAccessGUID");

                Dictionary<string, string> dictPermissionPermissionsData = null;
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    dictPermissionPermissionsData = new Dictionary<string, string>();
                    PermissionDataStorage.Add(storageKey, dictPermissionPermissionsData);
                }
                else
                {
                    dictPermissionPermissionsData = PermissionDataStorage[storageKey];
                }

                if (dictPermissionPermissionsData.ContainsKey(key))
                {
                    dictPermissionPermissionsData[key] = values;
                }
                else
                {
                    dictPermissionPermissionsData.Add(key, values);
                }

            }
            finally
            {
                if (lockWasTaken)
                {
                    Monitor.Exit(lockerRoleStation);
                }
            }
            return Ok();
        }

        public void btnRefreshRole_Click(string values)
        {
            var dh = new DataHelper(_configuration["ConnectionStrings:EmasConnection"]);
            var adConnectionString = dh.GetStringValueByProcedure("GetADConnString");
            var ad = new ActiveDirectory(adConnectionString, _db, _configuration);
            var dt = ad.GetGroups();
            dh.SaveDataByUdt("MergeADRoles", dt);

        }

        public void CopyRole(long o_roleId, long n_roleId)
        {

            var userId = HttpContext.Session.GetObjectFromJson<long>("UserID");

            using (NpgsqlConnection conn = _db.GetNpgsqlConnection())
            {
                using (var command = new NpgsqlCommand("copyrolerights", conn)
                {
                    CommandType = CommandType.StoredProcedure
                })
                {

                    command.Parameters.AddWithValue("o_roleid", o_roleId);
                    command.Parameters.AddWithValue("n_roleid", n_roleId);
                    command.Parameters.AddWithValue("v_userid", userId);


                    if (command.Connection.State == ConnectionState.Closed)
                        command.Connection.Open();

                    command.ExecuteNonQuery();

                    if (command.Connection.State != ConnectionState.Closed)
                        command.Connection.Close();

                }
            }



        }

        public string CopyRoleOlap(long src_roleId, long dst_roleId)
        { 
            string srcRole = _db.Roles.Select(f => new RoleDTO { Id = f.Id, Name = f.Name, Description = f.Description }).Where(f => f.Id == src_roleId).Select(f => f.Name).First();
            string dstRole = _db.Roles.Select(f => new RoleDTO { Id = f.Id, Name = f.Name, Description = f.Description }).Where(f => f.Id == dst_roleId).Select(f => f.Name).First();
            Schema objSchema = null;
            SchemaRole srcShemRole = null;
            SchemaRole dstShemRole = null;
            bool srcFlg=false;
            bool dstFlg=false;

            try
            {
                objSchema = getShema();
                foreach (SchemaRole itemShemRole in objSchema.Role.ToList()){ 
                    if(itemShemRole.name.Equals(srcRole) ){ 
                        srcShemRole =  itemShemRole;   
                        srcFlg = true;
                    }
                    if(itemShemRole.name.Equals(dstRole) ){ 
                        dstShemRole =  itemShemRole;   
                        dstFlg = true;
                    }
                    if(srcFlg && dstFlg) break;
                }
                if(!srcFlg){ 
                    return "Ошибка. Роль '" + srcRole + "' в OLAP-схеме не найдена";
                }
                if(!dstFlg){ 
                    return "Ошибка. Роль '" + dstRole + "' в OLAP-схеме не найдена";
                }

                copyRoleCubeAndHierarhyGrantAccess(srcShemRole, dstShemRole, objSchema);

            }
            catch (Exception ex)
            {
                return "Ошибка. " + ex.Message;
            }
            return "Скопировано";
        }

        private void copyRoleCubeAndHierarhyGrantAccess(SchemaRole srcShemRole, SchemaRole dstShemRole, Schema objSchema){ 

            fillMemberDictionary(objSchema, false);

            //Мапка доступа к кубам в роли источнике
            Dictionary<String, SchemaRoleSchemaGrantCubeGrantAccess> srcDictCubeGrantAccess = new Dictionary<String, SchemaRoleSchemaGrantCubeGrantAccess>();

            //Мапка прав доступа у иерархий в роли-источнике
            Dictionary<String, bool> srcDictDimensionGrantAccess = new Dictionary<String, bool>();

            //Мапка с кубами и иерархиями роли-источника
            //Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> srcDictCubeHierarhy = new Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>();
            Dictionary<String, List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> srcDictCubeHierarhy = new Dictionary<String, List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>();

            //Формируем мапки доступа кубов и иерархий у источника
            foreach(SchemaRoleSchemaGrantCubeGrant itemCube in srcShemRole.SchemaGrant[0].CubeGrant.ToList())
            {
                srcDictCubeGrantAccess.Add(itemCube.cube, itemCube.access);

                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> srcListHierarhy = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                srcDictCubeHierarhy.Add(itemCube.cube, srcListHierarhy);

                foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchy in itemCube.HierarchyGrant.ToList())
                {
                    SchemaRoleSchemaGrantCubeGrantHierarchyGrant newHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                    newHierarchy.hierarchy = itemHierarchy.hierarchy;
                    newHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                    newHierarchy.access = itemHierarchy.access;
                    srcListHierarhy.Add(newHierarchy);
                }
            }

            //формируем доступ к кубам у роли назначения
            for(int ind = 0; ind<dstShemRole.SchemaGrant[0].CubeGrant.Length; ind++)
            {
                dstShemRole.SchemaGrant[0].CubeGrant[ind].access = srcDictCubeGrantAccess[dstShemRole.SchemaGrant[0].CubeGrant[ind].cube];
            }

            //формируем доступ к иерархиям у роли назначения
            //Идем по роли-назначения
            //Исходим из того, что
            //Набор кубов одинаковый
            //Набор иерархий в кубах одинаковый

            foreach(SchemaRoleSchemaGrantCubeGrant dstItemCube in dstShemRole.SchemaGrant[0].CubeGrant.ToList())
            {
                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> srcListHierarhy = srcDictCubeHierarhy[dstItemCube.cube];
                dstItemCube.HierarchyGrant = srcListHierarhy.ToArray();
            }
            //Теги DimensionGrant сфорируются при обновлении страницы

            //Сохраняем
            schemaSave(objSchema);

        }
 
    }
}
